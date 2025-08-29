#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL + TimescaleDB backend for the data-bridge.

Mantém as funcionalidades do script original, alterando somente a camada de persistência.
"""

from __future__ import annotations

import os
import json
import time
import math
import logging
import threading
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import paho.mqtt.client as mqtt
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import psycopg
from psycopg.rows import tuple_row
try:
    from prometheus_client import Counter, Gauge, start_http_server
    _PROM_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - fallback for missing dependency
    _PROM_AVAILABLE = False

    class _DummyMetric:
        def inc(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

    def Counter(*args, **kwargs):  # type: ignore
        return _DummyMetric()

    Gauge = Counter  # type: ignore

    def start_http_server(*args, **kwargs):  # type: ignore
        logging.getLogger("data-process-pg").warning(
            "prometheus_client not installed; metrics disabled"
        )

# ---------------------------------------------------------------------------
# Dependências internas (mantidas)
# ---------------------------------------------------------------------------
from schema import validate_and_extract, get_by_path  # não alterar

# ---------------------------------------------------------------------------
# Config & Logging
# ---------------------------------------------------------------------------

load_dotenv()

def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}

@dataclass(frozen=True)
class Settings:
    # Postgres
    pg_dsn: Optional[str] = os.getenv("PG_DSN")
    pg_host: str = os.getenv("PGHOST", "127.0.0.1")
    pg_port: int = int(os.getenv("PGPORT", "5432"))
    pg_db: str = os.getenv("PGDATABASE", "databridge")
    pg_user: str = os.getenv("PGUSER", "databridge")
    pg_pass: str = os.getenv("PGPASSWORD", "")
    pg_connect_timeout: int = int(os.getenv("PGCONNECT_TIMEOUT", "10"))
    pg_target_schema: str = os.getenv("PGSCHEMA", "ingest")
    pg_table: str = os.getenv("PGTABLE", "readings")
    pg_stmt_timeout_ms: int = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "10000"))

    # Janela estatística
    lookback_business_days: int = int(os.getenv("LOOKBACK_BUSINESS_DAYS", "10"))
    lookback_weekend_days: int = int(os.getenv("LOOKBACK_WEEKEND_DAYS", os.getenv("LOOKBACK_BUSINESS_DAYS", "10")))
    min_days_for_stats: int = int(os.getenv("MIN_DAYS_FOR_STATS", "3"))
    sigma_multiplier: float = float(os.getenv("SIGMA_MULTIPLIER", "2.0"))
    min_sigma_floor: float = float(os.getenv("MIN_SIGMA_FLOOR", "0.0"))

    # MQTT
    mqtt_host: str = os.getenv("MQTT_BROKER_HOST", "localhost")
    mqtt_port: int = int(os.getenv("MQTT_BROKER_PORT", 1883))
    mqtt_user: Optional[str] = os.getenv("MQTT_USERNAME")
    mqtt_pass: Optional[str] = os.getenv("MQTT_PASSWORD")
    mqtt_topic_in: str = os.getenv("INPUT_TOPIC", "sensors/#")
    mqtt_keepalive: int = int(os.getenv("MQTT_KEEPALIVE", "60"))

    # MQTT TLS opcional
    mqtt_tls_enabled: bool = _env_bool("MQTT_TLS_ENABLED", "false")
    mqtt_tls_ca_certs: Optional[str] = os.getenv("MQTT_TLS_CA_CERTS")
    mqtt_tls_certfile: Optional[str] = os.getenv("MQTT_TLS_CERTFILE")
    mqtt_tls_keyfile: Optional[str] = os.getenv("MQTT_TLS_KEYFILE")
    mqtt_tls_insecure: bool = _env_bool("MQTT_TLS_INSECURE", "false")

    # Log
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # Região e TZ
    default_region: str = os.getenv("DEFAULT_REGION", "br_national")
    default_tz: str = os.getenv("DEFAULT_TZ", "America/Sao_Paulo")

    # Métricas/monitoramento
    metrics_port: int = int(os.getenv("METRICS_PORT", "8000"))
    metrics_log_interval: int = int(os.getenv("METRICS_LOG_INTERVAL_SEC", "60"))
    insert_stale_threshold: int = int(os.getenv("INSERT_STALE_THRESHOLD_SEC", "300"))


SETTINGS = Settings()

logging.basicConfig(
    level=SETTINGS.log_level,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("data-process-pg")

# Métricas Prometheus
INSERTIONS_COUNTER = Counter(
    "db_insertions_total", "Total de inserções bem-sucedidas"
)
LAST_INSERT_GAUGE = Gauge(
    "db_last_insert_timestamp", "Timestamp da última inserção (epoch)"
)

# ---------------------------------------------------------------------------
# Constantes de Domínio / Mapas Regionais
# ---------------------------------------------------------------------------

APPLICATION_REGION: Dict[str, str] = {
    # Campo Grande - MS
    "AGP 020": "campo_grande_ms",
    # adicione outros application_names aqui no futuro
}

REGION_TZ: Dict[str, str] = {
    "campo_grande_ms": "America/Campo_Grande",
    "br_national": SETTINGS.default_tz,
}

UTC = ZoneInfo("UTC")

# ---------------------------------------------------------------------------
# Infra: Banco de Dados (PostgreSQL + TimescaleDB)
# ---------------------------------------------------------------------------

class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.conn = self._connect()
        self._ensure_schema()
        self._ensure_indexes()

    def _dsn(self) -> str:
        if self.settings.pg_dsn:
            return self.settings.pg_dsn
        password_part = (
            f"password={self.settings.pg_pass} " if self.settings.pg_pass else ""
        )
        return (
            f"host={self.settings.pg_host} "
            f"port={self.settings.pg_port} "
            f"dbname={self.settings.pg_db} "
            f"user={self.settings.pg_user} "
            f"{password_part}"
            f"connect_timeout={self.settings.pg_connect_timeout} "
            f"options='-c statement_timeout={self.settings.pg_stmt_timeout_ms}'"
        )

    def _connect(self):
        # autocommit para operações isoladas de DDL e INSERT simples
        conn = psycopg.connect(self._dsn(), autocommit=True, row_factory=tuple_row)
        return conn

    def _reconnect(self) -> None:
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.conn = self._connect()

    def _ensure_schema(self) -> None:
        schema = self.settings.pg_target_schema
        table = self.settings.pg_table
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    id               BIGSERIAL PRIMARY KEY,
                    application_name TEXT,
                    device_profile   TEXT NOT NULL,
                    device_name      TEXT NOT NULL,
                    ts               TIMESTAMPTZ NOT NULL,
                    payload          JSONB NOT NULL
                );
            """)
            # hypertable (idempotente)
            cur.execute(f"""
                SELECT create_hypertable('{schema}.{table}','ts',
                                         chunk_time_interval => INTERVAL '7 days',
                                         if_not_exists => TRUE);
            """)

    def _ensure_indexes(self) -> None:
        schema = self.settings.pg_target_schema
        table = self.settings.pg_table
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_dev_ts ON {schema}.{table} (device_profile, device_name, ts DESC);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {schema}.{table} (ts DESC);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_payload_gin ON {schema}.{table} USING GIN (payload jsonb_path_ops);")

    def insert_reading(self, profile: str, device_name: str, application_name: str, ts_iso: str, payload_json: str) -> None:
        schema = self.settings.pg_target_schema
        table = self.settings.pg_table
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {schema}.{table} (device_profile, device_name, application_name, ts, payload) "
                f"VALUES (%s, %s, %s, %s, %s::jsonb)",
                (profile, device_name, application_name, ts_iso, payload_json)
            )

    def select_payloads_between(self, profile: str, device_name: str, start_iso: str, end_iso: str) -> List[Tuple[str, str]]:
        schema = self.settings.pg_target_schema
        table = self.settings.pg_table
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts::text, payload::text FROM {schema}.{table}
                 WHERE device_profile=%s AND device_name=%s AND ts>=%s AND ts<%s
                 ORDER BY ts ASC
                """,
                (profile, device_name, start_iso, end_iso)
            )
            return cur.fetchall()

    def select_last_payload_before(self, profile: str, device_name: str, ts_iso: str) -> Optional[str]:
        schema = self.settings.pg_target_schema
        table = self.settings.pg_table
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT payload::text FROM {schema}.{table}
                 WHERE device_profile=%s AND device_name=%s AND ts<%s
                 ORDER BY ts DESC LIMIT 1
                """,
                (profile, device_name, ts_iso)
            )
            row = cur.fetchone()
            return row[0] if row else None

# ---------------------------------------------------------------------------
# Calendário & TZ (mesma lógica do original)
# ---------------------------------------------------------------------------

class Calendar:
    @staticmethod
    @lru_cache(maxsize=64)
    def tz_for_region(region: str) -> ZoneInfo:
        tz_name = REGION_TZ.get(region) or SETTINGS.default_tz
        return ZoneInfo(tz_name)

    @staticmethod
    def is_business_day(d: date, region: str) -> bool:
        # Feriados básicos Brasil (fixos + móveis) — igual ao original
        easter = Calendar._easter_date(d.year)
        carnival_tuesday = easter - timedelta(days=47)
        carnival_monday = easter - timedelta(days=48)
        good_friday = easter - timedelta(days=2)
        corpus_christi = easter + timedelta(days=60)

        fixed = {
            date(d.year, 1, 1),
            date(d.year, 4, 21),
            date(d.year, 5, 1),
            date(d.year, 9, 7),
            date(d.year, 10, 12),
            date(d.year, 11, 2),
            date(d.year, 11, 15),
            date(d.year, 12, 25),
        }
        movable = {good_friday, corpus_christi, carnival_monday, carnival_tuesday}
        if d.weekday() >= 5:
            return False
        return d not in fixed.union(movable)

    @staticmethod
    def previous_business_days(ref: date, n: int, region: str) -> List[date]:
        out: List[date] = []
        cur = ref - timedelta(days=1)
        while len(out) < n:
            if Calendar.is_business_day(cur, region):
                out.append(cur)
            cur -= timedelta(days=1)
        return out

    @staticmethod
    def previous_weekend_days(ref: date, n: int) -> List[date]:
        out: List[date] = []
        cur = ref - timedelta(days=1)
        while len(out) < n:
            if cur.weekday() >= 5:
                out.append(cur)
            cur -= timedelta(days=1)
        return out

    @staticmethod
    def _easter_date(y: int) -> date:
        a = y % 19
        b = y // 100
        c = y % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        return date(y, month, day)

# ---------------------------------------------------------------------------
# Utilitários de faixa/consulta (mesma API do original)
# ---------------------------------------------------------------------------

def _utc_bounds_for_local_day(day_local: date, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    start_local = datetime(day_local.year, day_local.month, day_local.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return (start_local.astimezone(UTC), end_local.astimezone(UTC))

def fetch_day_payloads_local(db: Database, profile: str, device_name: str, day_local: date, tz: ZoneInfo) -> List[Tuple[datetime, Dict[str, Any]]]:
    start_utc, end_utc = _utc_bounds_for_local_day(day_local, tz)
    rows = db.select_payloads_between(profile, device_name, start_utc.isoformat(), end_utc.isoformat())
    out: List[Tuple[datetime, Dict[str, Any]]] = []
    for ts_str, p_str in rows:
        try:
            ts_utc = datetime.fromisoformat(ts_str)
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=UTC)
            ts_loc = ts_utc.astimezone(tz)
            out.append((ts_loc, json.loads(p_str)))
        except Exception:
            continue
    return out

def day_min_counter_and_daily_total(day_points: Iterable[Tuple[datetime, Dict[str, Any]]]) -> Tuple[Optional[float], Optional[float]]:
    min_counter: Optional[float] = None
    daily_total: Optional[float] = None
    for _, d in day_points:
        v = d.get("object_sensors_counter_flux_a")
        if v is not None:
            try:
                vv = float(v)
                min_counter = vv if min_counter is None else min(min_counter, vv)
            except Exception:
                pass
        dt = d.get("daily_consumption_counter_flux_a")
        if dt is not None:
            try:
                dtt = float(dt)
                daily_total = dtt if daily_total is None else max(daily_total, dtt)
            except Exception:
                pass
    return min_counter, daily_total

def hour_max_counter(day_points: Iterable[Tuple[datetime, Dict[str, Any]]], hour: int) -> Optional[float]:
    max_v: Optional[float] = None
    for ts_local, d in day_points:
        if ts_local.hour == hour:
            v = d.get("object_sensors_counter_flux_a")
            if v is None:
                continue
            try:
                vv = float(v)
            except Exception:
                continue
            max_v = vv if max_v is None else max(max_v, vv)
    return max_v

# ---------------------------------------------------------------------------
# Cálculos ITC200 (idênticos ao original, adaptando chamadas DB)
# ---------------------------------------------------------------------------

def compute_itc200_metrics(db: Database, device_profile: str, device_name: str, ts_iso: str, current_counter: float, tz: ZoneInfo) -> Tuple[Optional[float], Optional[float]]:
    prev_json = db.select_last_payload_before(device_profile, device_name, ts_iso)
    delta_hour: Optional[float] = None
    if prev_json:
        try:
            prev = json.loads(prev_json)
            prev_counter = prev.get("object_sensors_counter_flux_a")
            if prev_counter is not None:
                delta_hour = current_counter - float(prev_counter)
        except Exception:
            pass

    ts = datetime.fromisoformat(ts_iso)
    points_today = fetch_day_payloads_local(db, device_profile, device_name, ts.astimezone(tz).date(), tz)
    counters: List[float] = []
    for _, d in points_today:
        v = d.get("object_sensors_counter_flux_a")
        if v is None:
            continue
        try:
            counters.append(float(v))
        except Exception:
            pass
    counters.append(float(current_counter))
    daily = (max(counters) - min(counters)) if counters else None
    return delta_hour, daily

def _compute_hourly_expected_from_daily_core(
    db: Database,
    profile: str,
    device_name: str,
    ts_iso: str,
    application_name: str,
    day_list: List[date],
    min_days_for_stats: int,
    min_sigma_floor: float,
) -> Tuple[Optional[float], Optional[float], int, int, Optional[float], Optional[float]]:
    ts = datetime.fromisoformat(ts_iso)
    region = APPLICATION_REGION.get(application_name, SETTINGS.default_region)
    tz = Calendar.tz_for_region(region)
    ts_local = ts.astimezone(tz)
    hour = ts_local.hour

    daily_list: List[float] = []
    frac_list: List[float] = []
    expected_list: List[float] = []

    for d in day_list:
        points = fetch_day_payloads_local(db, profile, device_name, d, tz)
        if not points:
            continue
        min_counter, daily_total = day_min_counter_and_daily_total(points)
        if min_counter is None or not daily_total or daily_total <= 0:
            continue
        max_at_hour = hour_max_counter(points, hour)
        if max_at_hour is None:
            continue
        cum_at_hour = max(0.0, float(max_at_hour) - float(min_counter))
        fraction = min(1.0, max(0.0, cum_at_hour / float(daily_total)))
        daily_list.append(float(daily_total))
        frac_list.append(fraction)
        expected_list.append(float(daily_total) * fraction)

    if not expected_list:
        return None, None, 0, hour, None, None

    n = len(expected_list)
    baseline_daily = (sum(daily_list) / len(daily_list)) if daily_list else None
    baseline_fraction = (sum(frac_list) / len(frac_list)) if frac_list else None
    expected_mean = sum(expected_list) / n

    expected_sigma: Optional[float]
    if n < min_days_for_stats:
        expected_sigma = None
    else:
        var = sum((x - expected_mean) ** 2 for x in expected_list) / (n - 1) if n >= 2 else 0.0
        expected_sigma = math.sqrt(var)
        if expected_sigma < min_sigma_floor:
            expected_sigma = min_sigma_floor

    return baseline_daily, baseline_fraction, n, hour, expected_mean, expected_sigma

def compute_hourly_expected_from_daily_weekday(db: Database, profile: str, device_name: str, ts_iso: str, application_name: str, lookback_days: int) -> Tuple[Optional[float], Optional[float], int, int, Optional[float], Optional[float]]:
    ts = datetime.fromisoformat(ts_iso)
    region = APPLICATION_REGION.get(application_name, SETTINGS.default_region)
    tz = Calendar.tz_for_region(region)
    ts_local = ts.astimezone(tz)
    day_list = Calendar.previous_business_days(ts_local.date(), lookback_days, region)
    return _compute_hourly_expected_from_daily_core(db, profile, device_name, ts_iso, application_name, day_list, SETTINGS.min_days_for_stats, SETTINGS.min_sigma_floor)

def compute_hourly_expected_from_daily_weekend(db: Database, profile: str, device_name: str, ts_iso: str, application_name: str, lookback_days: int) -> Tuple[Optional[float], Optional[float], int, int, Optional[float], Optional[float]]:
    ts = datetime.fromisoformat(ts_iso)
    region = APPLICATION_REGION.get(application_name, SETTINGS.default_region)
    tz = Calendar.tz_for_region(region)
    ts_local = ts.astimezone(tz)
    day_list = Calendar.previous_weekend_days(ts_local.date(), lookback_days)
    return _compute_hourly_expected_from_daily_core(db, profile, device_name, ts_iso, application_name, day_list, SETTINGS.min_days_for_stats, SETTINGS.min_sigma_floor)

# ---------------------------------------------------------------------------
# Processamento de mensagens (idêntico ao original, com insert no PG)
# ---------------------------------------------------------------------------

class Processor:
    def __init__(self, db: Database):
        self.db = db
        self.insert_count = 0
        self.last_insert_ts: Optional[float] = None

    def handle_message(self, topic: str, payload_bytes: bytes) -> None:
        try:
            raw = json.loads(payload_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"JSON inválido em {topic}: {e}")
            return

        try:
            record = validate_and_extract(raw)
        except ValueError as e:
            logger.error(f"Payload rejeitado: {e}")
            return

        profile = record["device_profile"]

        ts_in = record.get("timestamp")
        dt = self._parse_ts(ts_in)
        ts_iso = dt.astimezone(UTC).isoformat()

        values: Dict[str, Any] = dict(record["values"])  # cópia

        device_name = get_by_path(raw, "deviceInfo.deviceName") or raw.get("deviceName")
        application_name = (
            values.get("deviceInfo_applicationName")
            or get_by_path(raw, "deviceInfo.applicationName")
            or ""
        )

        if not device_name or not application_name:
            logger.error(f"deviceName/applicationName ausentes no payload (profile={profile}); descartando.")
            return

        if profile == "ITC200":
            self._compute_itc200_enrich(profile, device_name, application_name, ts_iso, values)

        payload_json = json.dumps(values, ensure_ascii=False)

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self.db.insert_reading(profile, device_name, application_name, ts_iso, payload_json)
                self.insert_count += 1
                self.last_insert_ts = time.time()
                INSERTIONS_COUNTER.inc()
                LAST_INSERT_GAUGE.set(self.last_insert_ts)
                logger.debug(f"Persistido {profile}/{device_name} @ {ts_iso}")
                break
            except psycopg.OperationalError as e:
                logger.warning(
                    f"Erro operacional ao inserir {profile}/{device_name} @ {ts_iso} (tentativa {attempt}/{max_attempts}): {e}; reconectando"
                )
                self.db._reconnect()
                if attempt == max_attempts:
                    logger.error(
                        f"Máximo de tentativas atingido para {profile}/{device_name}@{ts_iso}"
                    )
            except Exception as e:
                logger.error(f"Erro ao inserir {profile}/{device_name}@{ts_iso}: {e}")
                break


    @staticmethod
    def _parse_ts(ts_in: Optional[str]) -> datetime:
        dt: Optional[datetime] = None
        if ts_in:
            try:
                dt = datetime.fromisoformat(ts_in)
            except Exception:
                dt = None
        if dt is None:
            dt = datetime.now(tz=UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def _compute_itc200_enrich(self, profile: str, device_name: str, application_name: str, ts_iso: str, values: Dict[str, Any]) -> None:
        region = APPLICATION_REGION.get(application_name, SETTINGS.default_region)
        tz = Calendar.tz_for_region(region)

        current = values.get("object_sensors_counter_flux_a")
        try:
            current_f = float(current) if current is not None else None
        except Exception:
            current_f = None

        if current_f is None:
            logger.warning("ITC200 sem object_sensors_counter_flux_a; pulando métricas.")
            return

        delta_h, daily = compute_itc200_metrics(self.db, profile, device_name, ts_iso, current_f, tz)
        values["delta_hour_counter_flux_a"] = delta_h
        values["daily_consumption_counter_flux_a"] = daily

        res_weekday = compute_hourly_expected_from_daily_weekday(self.db, profile, device_name, ts_iso, application_name, SETTINGS.lookback_business_days)
        res_weekend = compute_hourly_expected_from_daily_weekend(self.db, profile, device_name, ts_iso, application_name, SETTINGS.lookback_weekend_days)

        ts_local_now = datetime.fromisoformat(ts_iso).astimezone(tz)
        use_weekday = Calendar.is_business_day(ts_local_now.date(), region)
        active = res_weekday if use_weekday else res_weekend

        hour_local = ts_local_now.hour
        if active and len(active) == 6:
            baseline_daily, baseline_fraction, n_days, hour, expected_mean, expected_sigma = active
            hour_local = hour
        else:
            baseline_daily, baseline_fraction, n_days, expected_mean, expected_sigma = (None, None, 0, None, None)

        values["hour_index"] = hour_local
        values["hourly_baseline_n_days"] = n_days
        values["hourly_sigma_multiplier"] = SETTINGS.sigma_multiplier

        if (
            baseline_daily is not None and baseline_daily > 0
            and expected_mean is not None and expected_sigma is not None
        ):
            points_today = fetch_day_payloads_local(self.db, profile, device_name, datetime.fromisoformat(ts_iso).astimezone(tz).date(), tz)
            min_counter_today, _ = day_min_counter_and_daily_total(points_today)
            if min_counter_today is not None:
                cum_today = max(0.0, current_f - float(min_counter_today))
            else:
                cum_today = 0.0

            k = SETTINGS.sigma_multiplier
            upper = expected_mean + k * expected_sigma
            lower = max(0.0, expected_mean - k * expected_sigma)
            delta_pct = ((cum_today - expected_mean) / expected_mean * 100.0) if expected_mean > 0 else None
            zscore = ((cum_today - expected_mean) / expected_sigma) if (expected_sigma and expected_sigma > 0) else None

            values["hourly_baseline_daily_consumption"] = baseline_daily
            values["hourly_baseline_fraction_h"] = baseline_fraction
            values["hourly_expected_cumulative_at_hour"] = expected_mean
            values["hourly_expected_sigma"] = expected_sigma
            values["hourly_upper_2sigma"] = upper
            values["hourly_lower_2sigma"] = lower
            values["hourly_cumulative_today"] = cum_today
            values["hourly_delta_vs_expected_pct"] = delta_pct
            values["hourly_zscore"] = zscore
            values["hourly_anomaly_2sigma"] = (cum_today > upper) or (cum_today < lower)
        else:
            values["hourly_baseline_daily_consumption"] = None
            values["hourly_baseline_fraction_h"] = None
            values["hourly_expected_cumulative_at_hour"] = None
            values["hourly_expected_sigma"] = None
            values["hourly_upper_2sigma"] = None
            values["hourly_lower_2sigma"] = None
            values["hourly_cumulative_today"] = None
            values["hourly_delta_vs_expected_pct"] = None
            values["hourly_zscore"] = None
            values["hourly_anomaly_2sigma"] = None

        self._publish_alt_series(values, res_weekday, prefix="hourly_weekday_")
        self._publish_alt_series(values, res_weekend, prefix="hourly_weekend_")

    @staticmethod
    def _publish_alt_series(values: Dict[str, Any], res: Optional[Tuple], prefix: str) -> None:
        keys = [
            "baseline_daily_consumption",
            "baseline_fraction_h",
            "expected_cumulative_at_hour",
            "expected_sigma",
            "upper_2sigma",
            "lower_2sigma",
            "n_days",
        ]
        if not res or len(res) != 6:
            for k in keys:
                values[prefix + k] = None if k != "n_days" else 0
            return

        daily, frac, n, _hour, mean, sigma = res
        if mean is not None and sigma is not None:
            values[prefix + "baseline_daily_consumption"] = daily
            values[prefix + "baseline_fraction_h"] = frac
            values[prefix + "expected_cumulative_at_hour"] = mean
            values[prefix + "expected_sigma"] = sigma
            k = SETTINGS.sigma_multiplier
            values[prefix + "upper_2sigma"] = mean + k * sigma
            values[prefix + "lower_2sigma"] = max(0.0, mean - k * sigma)
            values[prefix + "n_days"] = n
        else:
            values[prefix + "baseline_daily_consumption"] = None
            values[prefix + "baseline_fraction_h"] = None
            values[prefix + "expected_cumulative_at_hour"] = None
            values[prefix + "expected_sigma"] = None
            values[prefix + "upper_2sigma"] = None
            values[prefix + "lower_2sigma"] = None
            values[prefix + "n_days"] = n

# ---------------------------------------------------------------------------
# Monitoramento e métricas
# ---------------------------------------------------------------------------

class MetricsMonitor:
    def __init__(self, processor: "Processor", settings: Settings):
        self.processor = processor
        self.interval = settings.metrics_log_interval
        self.threshold = settings.insert_stale_threshold
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while True:
            time.sleep(self.interval)
            count = self.processor.insert_count
            last = self.processor.last_insert_ts
            last_str = (
                datetime.fromtimestamp(last, tz=UTC).isoformat() if last else "n/a"
            )
            logger.info(
                f"stats insertions_total={count} last_insert={last_str}"
            )
            if last and (time.time() - last) > self.threshold:
                delta = int(time.time() - last)
                logger.warning(f"Sem novas inserções há {delta}s")

# ---------------------------------------------------------------------------
# MQTT Wiring
# ---------------------------------------------------------------------------

class MqttApp:
    def __init__(self, settings: Settings, processor: Processor):
        self.settings = settings
        self.processor = processor
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        if self.settings.mqtt_user and self.settings.mqtt_pass:
            self.client.username_pw_set(self.settings.mqtt_user, self.settings.mqtt_pass)

        if self.settings.mqtt_tls_enabled:
            try:
                self.client.tls_set(
                    ca_certs=self.settings.mqtt_tls_ca_certs,
                    certfile=self.settings.mqtt_tls_certfile,
                    keyfile=self.settings.mqtt_tls_keyfile,
                )
                self.client.tls_insecure_set(self.settings.mqtt_tls_insecure)
                logger.info("MQTT TLS habilitado.")
            except Exception as e:
                logger.error(f"Falha ao habilitar TLS no MQTT: {e}")

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info(f"Conectado ao MQTT {self.settings.mqtt_host}:{self.settings.mqtt_port}")
            client.subscribe(self.settings.mqtt_topic_in)
            logger.info(f"Inscrito em {self.settings.mqtt_topic_in}")
        else:
            logger.error(f"Falha na conexão MQTT: código {reason_code}")

    def _on_message(self, client: mqtt.Client, userdata, msg, properties=None):
        self.processor.handle_message(msg.topic, msg.payload)

    def _on_disconnect(self, client: mqtt.Client, userdata, reason_code, properties):
        logger.info(f"[MQTT] Disconnected; reason_code={reason_code}")

    def run_forever(self) -> None:
        self.client.connect(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=self.settings.mqtt_keepalive)
        self.client.loop_forever()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if _PROM_AVAILABLE:
        start_http_server(SETTINGS.metrics_port)
    else:
        logger.warning("prometheus_client not installed; metrics disabled")
    db = Database(SETTINGS)
    processor = Processor(db)
    MetricsMonitor(processor, SETTINGS)
    app = MqttApp(SETTINGS, processor)
    app.run_forever()

if __name__ == "__main__":
    main()
