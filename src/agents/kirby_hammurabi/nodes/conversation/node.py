import re
from langchain.chat_models import init_chat_model

from src.agents.kirby_hammurabi.state import State
from src.agents.kirby_hammurabi.nodes.conversation.prompt import prompt_template

NAMESPACE_PATTERN = re.compile(r"^pe\-.+\-\w{4}\d{4}$")
VALID_SIZES = {"S", "M", "L"}

REQUIRED_FIELDS = {
    "uuaa": "UUAA - Unidad Aplicativa (4 letras, ej: PMKD)",
    "raw_physical_name": "Nombre físico de la tabla raw (se extrae del datum raw)",
    "master_physical_name": "Nombre físico de la tabla master (se extrae del datum master)",
    "namespace": "Namespace del repositorio de configs (ej: pe-de-cpdin-inq-pmkd0000)",
    "source_delimiter": 'Delimitador del archivo CSV de origen (ej: ";", ",", "|")',
    "source_has_header": "Si el archivo de origen tiene cabecera (true/false)",
}

OPTIONAL_FIELDS = {
    "security_level": "Nivel de seguridad (L1, L2, L3) — se extrae del datum si está disponible",
    "source_charset": "Charset del archivo de origen (por defecto: UTF-8)",
    "job_size": "Tamaño del job en la plataforma: S, M o L (por defecto: S)",
    "concurrency": "Nivel de concurrencia del job (por defecto: 49)",
    "metaconfig_version_raw": 'Versión del metaConfig para componentes raw (por defecto: 0.1.0)',
    "metaconfig_version_master": 'Versión del metaConfig para componentes master (por defecto: 0.1.0)',
}


def _is_valid(field: str, value) -> bool:
    """Valida que el valor de un campo sea real y no inventado."""
    if value is None:
        return False
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return False
    if isinstance(value, bool):
        return True
    validators = {
        "uuaa": lambda v: bool(re.match(r"^[A-Z]{4}$", str(v).upper())),
        "namespace": lambda v: bool(NAMESPACE_PATTERN.match(v)),
        "job_size": lambda v: v.upper() in VALID_SIZES,
        "concurrency": lambda v: isinstance(v, int) and v > 0,
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
    optional_list = (
        "\n".join(f"- {desc}" for desc in missing_optional.values())
        if missing_optional
        else ""
    )

    llm = init_chat_model("openai:gpt-4o-mini", temperature=0.5)

    prompt = prompt_template.format(
        missing_fields=missing_list, optional_fields=optional_list
    )
    messages = [("system", prompt)] + state["messages"]
    response = llm.invoke(messages)

    return {"messages": [response]}
