# schema.py

import jsonschema
from jsonschema import validate, ValidationError

# JSON Schema definitions for each known device profile
SENSOR_SCHEMAS = {
    # Perfil NIT21LI
    "NIT21LI": {
        "fields": {
            "deviceInfo.applicationName":      True,
            "deviceInfo.deviceProfileName":    True,
            "deviceInfo.deviceName":           True,
            "time":                             True,
            #"object.probes_temperature":       False,  # v do probes[0]
            #"object.probes_u":                 False,  # u do probes[0]
            "object.drys_c1_state":            False,  # valor de drys n="c1_state"
            "object.drys_c2_state":            False,  # valor de drys n="c2_state"
            "object.internal_sensors_temperature":  True,  # v onde n=="temperature"
            "object.internal_sensors_humidity":     True,  # v onde n=="humidity"
            "object.internal_sensors_battery":      False,  # se existir
            "rxInfo.0.rssi":                       False
        }
    },
    # Perfil ITC200
    "ITC200": {
        "fields": {
            "deviceInfo.applicationName":            True,
            "deviceInfo.deviceProfileName":          True,
            "deviceInfo.deviceName":                 True,
            "time":                                  True,
            "object.sensors_counter_flux_a":         True,   # v onde n=="counter_flux_a"
            "object.sensors_counter_reflux_a":       True,   # v onde n=="counter_reflux_a"
            "object.sensors_internal_temperature":   False,  # v onde n=="internal_temperature"
            "object.sensors_internal_relative_humidity": False, # v onde n=="internal_relative_humidity"
            "object.sensors_battery_voltage":        False,  # v onde n=="battery_voltage"
            "object.sensors_meter_resolution":       False,  # v onde n=="meter_resolution"
            "rxInfo.0.rssi":                         False
        }
    },

    "ITE11LI": {
        "fields": {
            "deviceInfo.applicationName":            True,
            "deviceInfo.deviceProfileName":          True,
            "deviceInfo.deviceName":                 True,
            "time":                                  True,
            "object.sensors_temperature":            False,
            "object.sensors_frequency":              False,
            "object.sensors_phase_a_voltage":        False,
            "object.sensors_phase_a_current":        False,
            "object.sensors_phase_a_pwr_factor":     False,
            "object.sensors_phase_a_active_power":  False,
            "object.sensors_phase_a_reactive_power":False,
            "object.sensors_phase_b_voltage":        False,
            "object.sensors_phase_b_current":        False,
            "object.sensors_phase_b_pwr_factor":     False,
            "object.sensors_phase_b_active_power":  False,
            "object.sensors_phase_b_reactive_power":False,
            "object.sensors_phase_c_voltage":        False,
            "object.sensors_phase_c_current":        False,
            "object.sensors_phase_c_pwr_factor":     False,
            "object.sensors_phase_c_active_power":  False,
            "object.sensors_phase_c_reactive_power":False,            
            "object.sensors_total_active_energy":    False,
            "object.sensors_total_reactive_energy":  False,
            "rxInfo.0.rssi":                         False
        }
    },
    "DTL485-TC": {
        "fields": {
            "deviceInfo.applicationName":   True,
            "deviceInfo.deviceProfileName": True,
            "deviceInfo.deviceName":        True,
            "time":                         True,
    
            # leituras de corrente (obrigatórias)
            "object.Current1_A":            False,   # valor do canal 1
            "object.Current2_A":            False,   # valor do canal 2
            "object.Current3_A":            False,   # valor do canal 3
            "object.Current4_A":            False,   # valor do canal 4
    
            # demais campos de status (opcionais)
            "object.BatV":                  False,  # voltagem da bateria
            "object.EXTI_Level":            False,
            "object.EXTI_Trigger":          False,
            "object.Cur1H_status":          False,
            "object.Cur1L_status":          False,
            "object.Cur2H_status":          False,
            "object.Cur2L_status":          False,
            "object.Cur3H_status":          False,
            "object.Cur3L_status":          False,
            "object.Cur4H_status":          False,
            "object.Cur4L_status":          False,
    
            # sinal de recepção
            "rxInfo.0.rssi":                False
        }
    },
    "DTL300-Temp": {
        "fields": {
            "deviceInfo.applicationName":   True,
            "deviceInfo.deviceProfileName": True,
            "deviceInfo.deviceName":        True,
            "time":                         True,

            "object.BatV":   False,
            "object.TempC1": False,
            "object.TempC2": False,
            "object.TempC3": False,

            "rxInfo.0.rssi": False
        }
    }
}

def get_by_path(data: dict, path: str):
    keys = path.split('.')
    cur = data
    for k in keys:
        # Suporte a índice em listas, ex: 'rxInfo.0.rssi'
        if isinstance(cur, list):
            try:
                k = int(k)
            except ValueError:
                return None
            if len(cur) > k:
                cur = cur[k]
            else:
                return None
        elif isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur

def extract_from_list(lst: list, name: str):
    for item in lst:
        if item.get('n') == name:
            return item.get('v')
    return None

def expand_probes_in_values(payload: dict, extracted: dict, force_probe_1=False) -> dict:
    """
    Para cada probe em object.probes, adiciona chaves sequenciais.
    Se force_probe_1=True, garante que object_probe_1, object_probe_1_unit e object_probe_1_name existem (com None se não houver probe).
    """
    object_section = payload.get("object", {})
    probes = object_section.get("probes", [])
    for idx, probe in enumerate(probes, start=1):
        v = probe.get("v")
        u = probe.get("u")
        n = probe.get("n")
        key_value = f"object_probe_{idx}"
        key_unit  = f"object_probe_{idx}_unit"
        key_name  = f"object_probe_{idx}_name"
        extracted[key_value] = v
        extracted[key_unit]  = u
        extracted[key_name]  = n

    # Força a presença dos campos do primeiro probe, mesmo que não existam
    if force_probe_1:
        if "object_probe_1" not in extracted:
            extracted["object_probe_1"] = 0
            extracted["object_probe_1_unit"] = None
            extracted["object_probe_1_name"] = None

    return extracted

def validate_and_extract(payload: dict) -> dict:
    """
    Valida o payload e devolve:
        {
            "device_profile": <str>,
            "timestamp": <str>,
            "values": {chave_formatada: valor ou None},
            "probes": [ {name, value, unit}, ... ]   # <--- agora disponível!
        }
    Lança ValueError se faltar campo obrigatório.
    """

    profile = get_by_path(payload, "deviceInfo.deviceProfileName") \
              or payload.get("deviceProfileName")

    if profile not in SENSOR_SCHEMAS:
        raise ValueError(f"Perfil de dispositivo desconhecido: {profile}")

    schema = SENSOR_SCHEMAS[profile]
    extracted: dict[str, any] = {}

    for path, required in schema["fields"].items():
        val = None

        if path.startswith("object.sensors_"):
            key  = path[len("object.sensors_"):]            
            vals = get_by_path(payload, "object.sensors") or []
            val  = extract_from_list(vals, key)

        elif path.startswith("object.internal_sensors_"):
            key  = path[len("object.internal_sensors_"):]
            vals = get_by_path(payload, "object.internal_sensors") or []
            val  = extract_from_list(vals, key)

        elif path.startswith("object.drys_"):
            key  = path[len("object.drys_"):]
            vals = get_by_path(payload, "object.drys") or []
            val  = extract_from_list(vals, key)

        elif path.startswith("rxInfo."):
            parts = path.split('.')
            if len(parts) == 3:
                _, idx_str, attr = parts
                try:
                    idx = int(idx_str)
                except ValueError:
                    idx = 0
                rx = payload.get("rxInfo", [])
                if len(rx) > idx and isinstance(rx[idx], dict):
                    val = rx[idx].get(attr)
        else:
            val = get_by_path(payload, path)

        if required and val is None:
            raise ValueError(f"Campo obrigatório '{path}' ausente para perfil {profile}")

        extracted[path.replace(".", "_")] = val

    ts = get_by_path(payload, "time") or payload.get("timestamp")

    force_probe_1 = profile.startswith("NIT21")
    extracted = expand_probes_in_values(payload, extracted, force_probe_1=force_probe_1)

    return {
        "device_profile": profile,
        "timestamp": ts,
        "values": extracted,
    }
