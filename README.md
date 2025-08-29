# Data Bridge — PostgreSQL + TimescaleDB

Este pacote contém **manual passo a passo**, **scripts de banco**, **serviço** e **código** para migrar seu *data-bridge* de SQLite para **PostgreSQL 16 + TimescaleDB**, mantendo todas as funcionalidades atuais de ingestão e cálculo, mudando apenas a camada de persistência.

> Diretório sugerido: `~/projects/data-bridge-pgsql`

---

## 1) Instalação do PostgreSQL 16 + TimescaleDB

> Assumindo Ubuntu 22.04+ (ajuste conforme sua distro).

```bash
# PostgreSQL 16
sudo apt-get update
sudo apt-get install -y postgresql-16 postgresql-client-16

# TimescaleDB 2.x para PostgreSQL 16
# (Se necessário, adicione o repo oficial da Timescale)
# https://docs.timescale.com/self-hosted/latest/install/installation-apt-ubuntu/
# Exemplo:
# sudo add-apt-repository ppa:timescale/timescaledb-ppa
# sudo apt-get update
sudo apt-get install -y timescaledb-2-postgresql-16
```

Depois, habilite o TimescaleDB no `postgresql.conf` (método automático recomendado):

```bash
sudo timescaledb-tune --pg-config=/usr/lib/postgresql/16/bin/pg_config --yes
sudo systemctl restart postgresql
```

Confirme a versão:
```bash
psql --version
```

---

## 2) Provisionar banco e extensão

Ajuste o usuário/senha abaixo e execute:

```bash
sudo -u postgres psql <<'SQL'
DO $$ BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'databridge') THEN
      CREATE ROLE databridge WITH LOGIN PASSWORD 'TroqueEstaSenha';
   END IF;
END $$;

DO $$ BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'databridge') THEN
      CREATE DATABASE databridge OWNER databridge;
   END IF;
END $$;

\c databridge

-- Extensão TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Schema dedicado
CREATE SCHEMA IF NOT EXISTS ingest AUTHORIZATION databridge;

-- Tabela de leituras (TIMESTAMPTZ + JSONB)
CREATE TABLE IF NOT EXISTS ingest.readings (
  id                BIGSERIAL PRIMARY KEY,
  application_name  TEXT,
  device_profile    TEXT NOT NULL,
  device_name       TEXT NOT NULL,
  ts                TIMESTAMPTZ NOT NULL,
  payload           JSONB NOT NULL
);

-- Hiper-tabela com partição temporal (intervalo padrão: 7 dias)
SELECT create_hypertable('ingest.readings','ts',
                         chunk_time_interval => INTERVAL '7 days',
                         if_not_exists => TRUE);

-- Índices (consultas por perfil+dispositivo+tempo e por tempo)
CREATE INDEX IF NOT EXISTS idx_readings_dev_ts
  ON ingest.readings (device_profile, device_name, ts DESC);

CREATE INDEX IF NOT EXISTS idx_readings_ts
  ON ingest.readings (ts DESC);

-- Index JSONB (para consultas ad-hoc no payload)
CREATE INDEX IF NOT EXISTS idx_readings_payload_gin
  ON ingest.readings USING GIN (payload jsonb_path_ops);

-- Compressão e retenção (ajuste prazos conforme necessidade)
ALTER TABLE ingest.readings
  SET (timescaledb.compress,
       timescaledb.compress_orderby = 'ts DESC',
       timescaledb.compress_segmentby = 'device_profile, device_name');

-- Comprime dados com mais de 30 dias
SELECT add_compression_policy('ingest.readings', INTERVAL '30 days');

-- Retém por 365 dias; ajuste conforme seu SLO
SELECT add_retention_policy('ingest.readings', INTERVAL '365 days');

GRANT USAGE ON SCHEMA ingest TO databridge;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ingest TO databridge;
ALTER DEFAULT PRIVILEGES IN SCHEMA ingest GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO databridge;
SQL
```

> Se preferir, use o script pronto: `db/init_timescale.sql`

---

## 3) Variáveis de ambiente (.env)

Crie um arquivo `.env` (ou edite o seu) com as credenciais do PostgreSQL e seus parâmetros MQTT/TLS:

```bash
cp .env.example .env
vim .env
```

**Principais chaves**:
- `DB_BACKEND=postgres`
- `PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD` (ou `PG_DSN`)
- `INPUT_TOPIC`, `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_TLS_*`
- `METRICS_PORT` (porta HTTP para expor métricas Prometheus; padrão 8000)
- `METRICS_LOG_INTERVAL_SEC` (intervalo de log periódico; padrão 60)
- `INSERT_STALE_THRESHOLD_SEC` (alerta se sem inserções por X s; padrão 300)

---

## 4) Ambiente Python

Use venv:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 5) Migração do histórico (SQLite → PostgreSQL)

Execute o migrador apontando para o arquivo SQLite atual e para o PostgreSQL:

```bash
source .venv/bin/activate

python src/migrate_sqlite_to_pg.py   --sqlite /caminho/para/data_ingest.db   --pgdsn "host=127.0.0.1 port=5432 dbname=databridge user=databridge password=TroqueEstaSenha"   --batch-size 10000   --since "2020-01-01T00:00:00Z"
```

Observações:
- A migração é *idempotente* por (id,ts,device_*). Se você rodar novamente, as linhas iguais são ignoradas (veja a *upsert* no script).
- `--since` é opcional. Sem ele, migra todo o acervo.

Para validar paridade de dados:
```bash
python src/verify_parity.py   --sqlite /caminho/para/data_ingest.db   --pgdsn "host=127.0.0.1 port=5432 dbname=databridge user=databridge password=TroqueEstaSenha"   --days 7
```

---

## 6) Subir o novo serviço em paralelo

Rode o novo processo que escreve no PostgreSQL sem desligar o serviço atual (SQLite):

```bash
source .venv/bin/activate
python src/data_process_pg.py
```

> Ele assina o mesmo tópico MQTT, e grava no `ingest.readings` (TimescaleDB).

### systemd (opcional)
Ajuste o arquivo `systemd/data-bridge-pgsql.service` e instale:

```bash
sudo cp systemd/data-bridge-pgsql.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable data-bridge-pgsql
sudo systemctl start data-bridge-pgsql
sudo systemctl status data-bridge-pgsql
```

---

## 7) Cutover e limpeza

1. Monitore por algumas semanas a paridade (contagem diária e amostras).
2. Direcione as consultas/relatórios para o PostgreSQL.
3. Desligue o serviço antigo.
4. (Opcional) Arquive o SQLite e remova do host.

---

## 8) Consultas úteis (validação / observabilidade)

Total por dia (UTC):
```sql
SELECT date_trunc('day', ts) AS d, COUNT(*) AS n
FROM ingest.readings
GROUP BY 1 ORDER BY 1 DESC LIMIT 30;
```

Amostra de um dispositivo e janela:
```sql
SELECT device_profile, device_name, ts, payload
FROM ingest.readings
WHERE device_profile = 'ITC200'
  AND device_name = 'AGP 020'
  AND ts >= now() - interval '2 days'
ORDER BY ts ASC LIMIT 100;
```

Checar políticas Timescale:
```sql
SELECT * FROM timescaledb_information.jobs ORDER BY id;
```

---

## 9) Ponderações de arquitetura (nível sênior)

- **Conexão**: use `psycopg3` com *retries* exponenciais, *statement timeouts* (ex.: 5–10s) e *pooling* via **pgBouncer** em modo *transaction* para estabilidade sob carga.
- **Tamanho do *chunk***: `7 days` é ponto de partida. Ajuste conforme cardinalidade/volume; *chunks* devem caber na memória de trabalho do servidor para *queries* comuns.
- **Retenção/Compressão**: polícias já incluídas. Ajuste SLO de retenção e *compress_orderby/segmentby* para acelerar *scans* por dispositivo.
- **Índices**: não exagere. Mantenha `(device_profile, device_name, ts)` como primário para *queries* correntes; adicione GIN para rare *ad-hoc* no `payload`.
- **Backups**: `pg_dump` rotineiro + *WAL archiving* (ou *physical base backups*) para RPO ~0. **Teste restore**.
- **HA**: se necessário, *replicação* assíncrona com um *standby* e `max_wal_senders >= 10`.
- **Telemetria**: habilitar `pg_stat_statements`, coletar métricas com Prometheus/Grafana e alertas para *lag* de ingestão e falhas MQTT.
- **Segurança**: role `databridge` com *least privilege*; TLS no MQTT (já suportado no código) e TLS no Postgres (`sslmode=require`, `sslrootcert`).

Boa migração!
