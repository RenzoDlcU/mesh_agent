from typing import Optional, List, Dict
from langgraph.graph import MessagesState


class State(MessagesState):
    # Archivos datum subidos por el usuario
    uploaded_files: Optional[List[Dict[str, str]]]  # [{"name": "file.datum", "content": "..."}]

    # --- Datos extraídos del datum RAW ---
    uuaa: Optional[str]  # ej: "pmkd"
    raw_physical_name: Optional[str]  # ej: "t_kmux_murex_mk_vl_mov"
    raw_table: Optional[str]  # ej: "pe_raw.t_kmux_murex_mk_vl_mov"
    raw_path: Optional[str]  # ej: "/data/raw/kmux/data/t_kmux_murex_mk_vl_mov"
    source_path: Optional[str]  # ej: '/in/staging/datax/pmkd/VAR_RC_VM_"${?DATE}".csv'
    schema_path_raw: Optional[str]  # path al schema raw
    source_file_type: Optional[str]  # ej: "CSV"
    source_system_code: Optional[str]  # ej: "KMUX"
    partitions: Optional[str]  # ej: "cutoff_date"
    security_level: Optional[str]  # ej: "L1"

    # --- Datos extraídos del datum MASTER ---
    master_physical_name: Optional[str]  # ej: "t_pmkd_murex_mk_vl_mov"
    master_table: Optional[str]  # ej: "pe_master.t_pmkd_murex_mk_vl_mov"
    master_path: Optional[str]  # ej: "/data/master/pmkd/data/t_pmkd_murex_mk_vl_mov"
    schema_path_master: Optional[str]  # path al schema master

    # --- Datos extraídos del schema MASTER ---
    # [{"name": "master_col", "legacyName": "raw_col", "type": "string", ...}]
    schema_fields: Optional[List[Dict[str, str]]]

    # --- Parámetros proporcionados por el usuario ---
    namespace: Optional[str]  # ej: "pe-de-cpdin-inq-pmkd0000"
    source_delimiter: Optional[str]  # ej: ";"
    source_charset: Optional[str]  # ej: "UTF-8"
    source_has_header: Optional[bool]  # ej: True
    job_size: Optional[str]  # S, M, L
    concurrency: Optional[int]  # ej: 49
    metaconfig_version_raw: Optional[str]  # ej: "0.1.2"
    metaconfig_version_master: Optional[str]  # ej: "0.1.3"

    # --- Archivos generados (output) ---
    generated_files: Optional[List[Dict[str, str]]]  # [{"name": "file.conf", "content": "..."}]
