"""
Parser de archivos .datum (metadatos de Governance de BBVA).

Extrae información de tablas raw y master desde los JSON de Datum
para alimentar la generación de ficheros Kirby (.conf) y Hammurabi (.json).
"""

import json
import re


def _clean_source_path(raw_source_path: str) -> str:
    """Limpia el sourcePath del datum raw eliminando escapes innecesarios."""
    # El sourcePath viene con comillas escapadas: /in/staging/datax/pmkd/VAR_RC_VM_"${?DATE}".csv
    # Lo dejamos tal cual ya que es el formato que usan los .conf
    return raw_source_path


def _detect_zone(path: str) -> str:
    """Detecta si un path corresponde a raw o master."""
    if "/raw/" in path:
        return "raw"
    if "/master/" in path:
        return "master"
    return "unknown"


def parse_datum_file(content: str) -> dict:
    """
    Parsea un archivo .datum y extrae metadatos relevantes.

    Returns:
        Dict con campos extraídos según el tipo (raw o master).
    """
    data = json.loads(content)
    inner = data.get("data", {}).get("data", {})

    result = {}

    physical_name = inner.get("physicalName", "")
    path = inner.get("path", "")
    zone = _detect_zone(path)

    result["zone"] = zone
    result["physical_name"] = physical_name
    result["path"] = path
    result["system_code"] = inner.get("systemCode", "")
    result["schema_path"] = inner.get("schemaPath", "")
    result["security_level"] = inner.get("securityLevel", "")
    result["partitions"] = inner.get("partitions", "")
    result["source_file_type"] = inner.get("sourceFileTypeName", "")
    result["source_path"] = inner.get("sourcePath", "")
    result["source_physical_name"] = inner.get("sourcePhysicalName", "")
    result["source_system_code"] = inner.get("sourceSystemCode", "")
    result["storage_type_name"] = inner.get("storageTypeName", "")
    result["storage_zone_type_name"] = inner.get("storageZoneTypeName", "")
    result["model_code_type"] = inner.get("modelCodeType", "")
    result["version"] = inner.get("version", "")

    return result


def _apply_common_fields(parsed: dict, state_updates: dict) -> None:
    """Aplica campos comunes (uuaa, security_level, partitions) desde un datum parseado."""
    if parsed.get("system_code") and "uuaa" not in state_updates:
        state_updates["uuaa"] = parsed["system_code"].upper()
    if parsed.get("security_level") and "security_level" not in state_updates:
        state_updates["security_level"] = parsed["security_level"]
    if parsed.get("partitions") and "partitions" not in state_updates:
        state_updates["partitions"] = parsed["partitions"]


def _apply_raw_fields(parsed: dict, state_updates: dict) -> None:
    """Aplica campos específicos de un datum raw."""
    state_updates["raw_physical_name"] = parsed["physical_name"]
    state_updates["raw_table"] = f"pe_raw.{parsed['physical_name']}"
    state_updates["raw_path"] = parsed["path"]
    state_updates["source_path"] = _clean_source_path(parsed["source_path"])
    state_updates["schema_path_raw"] = parsed["schema_path"]
    state_updates["source_file_type"] = parsed.get("source_file_type", "")
    state_updates["source_system_code"] = parsed.get("source_system_code", "")


def _apply_master_fields(parsed: dict, state_updates: dict) -> None:
    """Aplica campos específicos de un datum master."""
    state_updates["master_physical_name"] = parsed["physical_name"]
    state_updates["master_table"] = f"pe_master.{parsed['physical_name']}"
    state_updates["master_path"] = parsed["path"]
    state_updates["schema_path_master"] = parsed["schema_path"]


_ZONE_HANDLERS = {
    "raw": _apply_raw_fields,
    "master": _apply_master_fields,
}


def parse_schema_file(content: str) -> list[dict]:
    """
    Parsea un archivo .schema y extrae los campos con su mapeo legacy→master.

    Excluye campos calculados (legacyName == "Calculated") como cutoff_date y audtiminsert_date
    ya que se generan automáticamente en las transformaciones.

    Returns:
        Lista de dicts con name, legacyName, type por cada campo.
    """
    data = json.loads(content)
    fields = []
    for field in data.get("fields", []):
        legacy = field.get("legacyName", "")
        if legacy == "Calculated" or field.get("deleted", False) or field.get("metadata", False):
            continue
        field_type = field.get("type", "")
        # type puede ser ["string", "null"] o "string"
        if isinstance(field_type, list):
            field_type = field_type[0]
        fields.append({
            "name": field.get("name", ""),
            "legacyName": legacy,
            "type": field_type,
        })
    return fields


def extract_from_datum_files(files: list[dict]) -> dict:
    """
    Extrae datos del state a partir de archivos .datum y .schema subidos.

    Args:
        files: Lista de dicts con {"name": "archivo.datum", "content": "contenido JSON"}

    Returns:
        Dict con campos del state que se pueden auto-poblar.
    """
    state_updates = {}

    for f in files:
        name = f.get("name", "")
        content = f.get("content", "")

        if name.endswith(".schema"):
            try:
                schema_fields = parse_schema_file(content)
                if schema_fields:
                    state_updates["schema_fields"] = schema_fields
            except (json.JSONDecodeError, KeyError):
                pass
            continue

        if not name.endswith(".datum"):
            continue

        try:
            parsed = parse_datum_file(content)
        except (json.JSONDecodeError, KeyError):
            continue

        _apply_common_fields(parsed, state_updates)

        handler = _ZONE_HANDLERS.get(parsed.get("zone", ""))
        if handler:
            handler(parsed, state_updates)

    return state_updates
