"""
Nodo que procesa archivos .datum subidos por el usuario
para extraer metadatos de tablas raw y master automáticamente.

Campos que se auto-extraen:
    Desde raw.datum: uuaa, raw_physical_name, raw_table, raw_path,
                     source_path, schema_path_raw, source_file_type,
                     source_system_code, security_level, partitions
    Desde master.datum: master_physical_name, master_table, master_path,
                        schema_path_master

Solo se ejecuta si hay archivos en state["uploaded_files"].
No sobreescribe campos ya definidos en el estado.
"""

from src.agents.kirby_hammurabi.state import State
from src.services.datum_parser import extract_from_datum_files


def file_parser(state: State) -> dict:
    """Parsea archivos datum y extrae metadatos para generar Kirby/Hammurabi."""
    uploaded_files = state.get("uploaded_files")
    if not uploaded_files:
        return {}

    extracted = extract_from_datum_files(uploaded_files)

    # Solo poblar campos que aún no tengan valor en el state
    updates = {}
    for key, value in extracted.items():
        if not state.get(key):
            updates[key] = value

    if updates:
        fields_log = ", ".join(f"{k}={v}" for k, v in updates.items())
        print(f"✓ Datos extraídos de datum: {fields_log}")
    else:
        print("ℹ No se extrajeron datos nuevos de los archivos datum.")

    return updates
