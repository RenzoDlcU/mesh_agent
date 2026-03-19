import re
from langchain.chat_models import init_chat_model

from src.agents.mesh.state import State
from src.agents.mesh.nodes.conversation.prompt import prompt_template

VALID_PERIODICITIES = {"diaria", "semanal", "mensual"}
VALID_SECURITY_LEVELS = {"L1", "L2", "L3"}
UUAA_PATTERN = re.compile(r'^[A-Z]{4}$')
EMAIL_PATTERN = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
REGISTRO_PATTERN = re.compile(r'^[A-Z]\d{6}$')
NAMESPACE_PATTERN = re.compile(r'^pe\..+\.pro$')

REQUIRED_FIELDS = {
    "periodicity": "Periodicidad (diaria, semanal, mensual)",
    "uuaa": "UUAA - Unidad Aplicativa (4 letras, ej: PPAD, PBIL)",
    "security_level": "Nivel de seguridad (L1, L2 o L3)",
    "email_error": "Email para notificaciones de error ej: renzo.delacruz@bbva.com",
    "registro": "Registro del creador (ej: P034367)",
    "input_transmitted": "Nombre del insumo transmitido ej: H_MP_MEDIOS_PAGO_T",
    "table_name_raw": "Nombre de la tabla raw",
    "table_name_master": "Nombre de la tabla master",
    "datax_name": "Nombre del componente DataX",
    "datax_namespace": "Namespace de DataX",
    "components_namespace": "Namespace de componentes Kirby/Hammurabi",
    "hammurabi_staging": "Nombre del job Hammurabi staging",
    "hammurabi_raw": "Nombre del job Hammurabi raw",
    "hammurabi_master": "Nombre del job Hammurabi master",
    "kirby_raw": "Nombre del job Kirby raw",
    "kirby_master": "Nombre del job Kirby master",
    "user_story": "Historia de usuario asociada (ej: DEDFTRANSV-10074, DEGENAIPRO-26)",
    "datax_source_params": "Al menos 1 parámetro de ORIGEN del DataX (ej: ODATE con fecha de hoy YYYYMMDD)",
    "datax_destination_params": "Al menos 1 parámetro de DESTINO del DataX (ej: ODATE con fecha de hoy YYYYMMDD)",
    "component_params": "Al menos 1 parámetro para los componentes Kirby y Hammurabi (ej: ODATE, CUTOFF_DATE)",
}

OPTIONAL_FIELDS = {
    "execution_time": "Hora de ejecución en formato HHMM (ej: 0300 para las 3 AM)",
    "email_cc_error": "Emails CC para notificaciones de error",
    "order_date": "Fecha de orden (0: fecha del día, 1: fecha anterior)",
    "is_habile": "Si se ejecuta en día hábil (mencionar 'día hábil' si aplica)",
    "days": "Días específicos de ejecución (ej: D1, D15 para hábiles o 1, 15 para calendario)",
    "hammurabi_l1t": "Nombre del job Hammurabi L1T (solo si seguridad es L2)",
    "kirby_l1t": "Nombre del job Kirby L1T (solo si seguridad es L2)",
}


def _is_valid(field: str, value) -> bool:
    """Valida que el valor de un campo sea real y no inventado."""
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return False
    validators = {
        "periodicity": lambda v: v.lower() in VALID_PERIODICITIES,
        "uuaa": lambda v: bool(UUAA_PATTERN.match(v.upper())),
        "security_level": lambda v: v.upper() in VALID_SECURITY_LEVELS,
        "email_error": lambda v: bool(EMAIL_PATTERN.match(v)),
        "registro": lambda v: bool(REGISTRO_PATTERN.match(v.upper())),
        "datax_namespace": lambda v: bool(NAMESPACE_PATTERN.match(v)),
        "components_namespace": lambda v: bool(NAMESPACE_PATTERN.match(v)),
    }
    validator = validators.get(field)
    return validator(value) if validator else True


def get_missing_fields(state: State) -> dict[str, str]:
    """Retorna los campos requeridos que faltan o tienen valores inválidos."""
    missing = {}
    for field, description in REQUIRED_FIELDS.items():
        value = state.get(field)
        if not _is_valid(field, value):
            missing[field] = description
    return missing


def get_missing_optional_fields(state: State) -> dict[str, str]:
    """Retorna los campos opcionales que no se han proporcionado."""
    missing = {}
    for field, description in OPTIONAL_FIELDS.items():
        if not state.get(field):
            missing[field] = description
    return missing


def conversation(state: State):
    """Genera una respuesta pidiendo los campos faltantes al usuario."""
    missing = get_missing_fields(state)
    missing_list = "\n".join(f"- {desc}" for desc in missing.values())

    missing_optional = get_missing_optional_fields(state)
    optional_list = "\n".join(f"- {desc}" for desc in missing_optional.values()) if missing_optional else ""

    llm = init_chat_model("openai:gpt-4o-mini", temperature=0.5)

    prompt = prompt_template.format(missing_fields=missing_list, optional_fields=optional_list)
    messages = [("system", prompt)] + state["messages"]
    response = llm.invoke(messages)

    return {"messages": [response]}
