"""
Parser de archivos de configuración Kirby (.conf) y Hammurabi (.json)
para extraer datos útiles para la generación de mallas Control-M.

Archivos soportados:
    - JSON: Definiciones de jobs Hammurabi/Kirby (contienen jobName, runtime, metaConfig)
    - CONF: Configuraciones HOCON de Kirby/Hammurabi (contienen tablas, paths, etc.)
"""

import json
import re


def parse_job_json(content: str) -> dict:
    """
    Parsea un JSON de definición de job Kirby/Hammurabi.

    Extrae:
        - uuaa: Primeros 4 caracteres del jobName
        - phase: Fase del job (hammurabi_staging, kirby_raw, etc.)
        - job_name: Nombre completo del job

    La fase se determina por el sufijo del config name en metaConfig:
        - -qls- → hammurabi_staging
        - -qlr- → hammurabi_raw
        - -qlm- → hammurabi_master
        - -inr- → kirby_raw
        - -inm- → kirby_master
    """
    data = json.loads(content)
    job_name = data.get("jobName", "")
    runtime = data.get("runtime", "")
    meta_config = data.get("params", {}).get("metaConfig", "")

    result = {"job_name": job_name, "runtime": runtime}

    if len(job_name) >= 4:
        result["uuaa"] = job_name[:4].upper()

    # Fase desde metaConfig: "pe:uuaa:namespace:config_name:version"
    phase_map = {
        "-qls-": "hammurabi_staging",
        "-qlr-": "hammurabi_raw",
        "-qlm-": "hammurabi_master",
        "-inr-": "kirby_raw",
        "-inm-": "kirby_master",
    }

    if meta_config:
        parts = meta_config.split(":")
        if len(parts) >= 4:
            config_name = parts[3]
            for suffix, phase in phase_map.items():
                if suffix in config_name:
                    result["phase"] = phase
                    break

    return result


def parse_conf_file(content: str) -> dict:
    """
    Parsea un archivo HOCON de configuración Kirby/Hammurabi.

    Extrae:
        - table_raw: Tabla raw de salida (pe_raw.xxx)
        - table_master: Tabla master de salida (pe_master.xxx)
        - file_name: Nombre base del fichero de entrada (para input_transmitted)
    """
    result = {}

    # Extraer tablas de output: table = "pe_raw.xxx" o table = "pe_master.xxx"
    # Usa 'table' singular (output) y no 'tables' plural (input)
    for match in re.finditer(r'\btable\s*=\s*"(pe_(?:raw|master)\.\w+)"', content):
        table = match.group(1)
        if table.startswith("pe_raw."):
            result["table_raw"] = table
        elif table.startswith("pe_master."):
            result["table_master"] = table

    # Extraer paths de input (fichero CSV que se transmite)
    path_match = re.search(r'paths\s*=\s*\[\s*"([^"]+)"', content)
    if path_match:
        raw_path = path_match.group(1)  # ej: /in/staging/datax/pmkd/VAR_RC_VM_
        result["input_path"] = raw_path
        # Extraer nombre base del fichero (último segmento, sin trailing _)
        segments = raw_path.rstrip("/").split("/")
        if segments:
            result["file_name"] = segments[-1].rstrip("_")

    return result


def _process_json_file(content: str, state_updates: dict) -> None:
    """Procesa un archivo JSON de job y actualiza state_updates in-place."""
    try:
        parsed = parse_job_json(content)
    except (json.JSONDecodeError, KeyError):
        return

    if "uuaa" in parsed and "uuaa" not in state_updates:
        state_updates["uuaa"] = parsed["uuaa"]

    phase = parsed.get("phase")
    if phase and parsed.get("job_name"):
        state_updates[phase] = parsed["job_name"]


def _process_conf_file(content: str, state_updates: dict) -> None:
    """Procesa un archivo CONF de Kirby/Hammurabi y actualiza state_updates in-place."""
    try:
        parsed = parse_conf_file(content)
    except Exception:
        return

    if parsed.get("table_raw"):
        state_updates["table_name_raw"] = parsed["table_raw"]
    if parsed.get("table_master"):
        state_updates["table_name_master"] = parsed["table_master"]
    if parsed.get("file_name") and "input_transmitted" not in state_updates:
        state_updates["input_transmitted"] = parsed["file_name"]


def extract_from_config_files(files: list[dict]) -> dict:
    """
    Extrae datos de estado para la malla a partir de archivos de configuración.

    Args:
        files: Lista de dicts con {"name": "archivo.json", "content": "contenido"}

    Returns:
        Dict con campos del state que se pueden auto-poblar:
            uuaa, hammurabi_staging, hammurabi_raw, hammurabi_master,
            kirby_raw, kirby_master, table_name_raw, table_name_master,
            input_transmitted
    """
    state_updates = {}

    for f in files:
        name = f.get("name", "")
        content = f.get("content", "")

        if name.endswith(".json"):
            _process_json_file(content, state_updates)
        elif name.endswith(".conf"):
            _process_conf_file(content, state_updates)

    return state_updates
