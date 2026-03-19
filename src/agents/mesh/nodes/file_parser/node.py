"""
Nodo que procesa archivos de configuración Kirby/Hammurabi
subidos por el usuario para extraer datos de la malla automáticamente.

Campos que se auto-extraen:
    Desde JSON: uuaa, hammurabi_staging, hammurabi_raw, hammurabi_master,
                kirby_raw, kirby_master
    Desde CONF: table_name_raw, table_name_master, input_transmitted

Solo se ejecuta si hay archivos en state["uploaded_files"].
No sobreescribe campos ya definidos en el estado.
"""

from src.agents.mesh.state import State
from src.services.config_parser import extract_from_config_files


def file_parser(state: State) -> dict:
    """Parsea archivos de configuración y extrae datos para la malla."""
    uploaded_files = state.get("uploaded_files")
    if not uploaded_files:
        return {}

    extracted = extract_from_config_files(uploaded_files)

    # Solo poblar campos que aún no tengan valor en el state
    updates = {}
    for key, value in extracted.items():
        if not state.get(key):
            updates[key] = value

    if updates:
        fields_log = ", ".join(f"{k}={v}" for k, v in updates.items())
        print(f"✓ Datos extraídos de archivos: {fields_log}")
    else:
        print("ℹ No se extrajeron datos nuevos de los archivos.")

    return updates
