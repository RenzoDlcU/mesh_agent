from typing import Optional, List, Any, Dict
from langgraph.graph import MessagesState


class State(MessagesState):
    # Archivos de configuración subidos por el usuario
    uploaded_files: Optional[List[Dict[str, str]]]  # [{"name": "file.json", "content": "..."}]

    # Información de la malla de Control M
    periodicity: Optional[str]
    execution_time: Optional[str]
    uuaa: Optional[str]
    security_level: Optional[str]
    email_error: Optional[str]
    order_date: Optional[int]
    registro: Optional[str]
    components_namespace: Optional[str]
    is_habile: Optional[bool]
    days: Optional[str]
    email_cc_error: Optional[str]

    input_transmitted: Optional[str]
    table_name_raw: Optional[str]
    table_name_master: Optional[str]

    # Información de DataX
    datax_name: Optional[str]
    datax_namespace: Optional[str]
    datax_source_params: Optional[List[Any]]
    datax_destination_params: Optional[List[Any]]

    # Parámetros comunes para todos los componentes Kirby y Hammurabi
    component_params: Optional[List[Any]]

    # Información de Hammurabi
    hammurabi_staging: Optional[str]
    hammurabi_raw: Optional[str]
    hammurabi_master: Optional[str]
    hammurabi_l1t: Optional[str]

    # Información de Kirby
    kirby_raw: Optional[str]
    kirby_master: Optional[str]
    kirby_l1t: Optional[str]

    # Bitbucket - Correlativos dinámicos
    scope: Optional[str]  # "Local" o "Global"
    next_correlatives: Optional[Dict[str, int]]  # {"T": 1, "V": 4, "C": 3, "D": 2}
    user_story: Optional[str]  # ej: "DEDFTRANSV-10074"

    # Bitbucket - Malla destino para insertar jobs
    target_mesh_file: Optional[str]  # ruta en repo de malla existente con capacidad
    target_mesh_content: Optional[str]  # contenido XML de la malla destino
    target_mesh_job_count: Optional[int]  # cantidad actual de jobs en la malla destino

    # Generador - Output
    parent_folder: Optional[str]  # ej: "CR-PEPADMEN-T02"
    control_m_xml: Optional[str]

    # Bitbucket - PR
    pr_url: Optional[str]
