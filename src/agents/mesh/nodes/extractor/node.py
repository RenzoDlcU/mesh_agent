from typing import List, Optional
from langchain.chat_models import init_chat_model
from pydantic import BaseModel, Field

from src.agents.mesh.state import State
from src.agents.mesh.nodes.extractor.prompt import prompt_template


class ComponentParam(BaseModel):
    """Parámetro de un componente."""
    name: str = Field(description="Nombre del parámetro (libre, ej: ODATE, PROCESS_DATE, PORT)")
    value: Optional[str] = Field(default=None, description="Valor fijo si pattern es FIXED")
    pattern: Optional[str] = Field(default=None, description="Patrón de cálculo: TODAY_YYYYMMDD, TODAY_YYYY-MM-DD, YESTERDAY_YYYYMMDD, LAST_DAY_PREV_MONTH, FIXED, etc.")


class MeshInformation(BaseModel):
    """Información para la malla de Control M."""

    periodicity: Optional[str] = Field(default=None, description="Periodicidad (diaria, semanal, mensual)")
    execution_time: Optional[str] = Field(default=None, description="Hora de ejecución (HHMM)")
    uuaa: Optional[str] = Field(default=None, description="Unidad aplicativa (4 letras)")
    security_level: Optional[str] = Field(default=None, description="Nivel de seguridad (L1, L2, L3)")
    email_error: Optional[str] = Field(default=None, description="Email para errores")
    order_date: Optional[int] = Field(default=None, description="Fecha de orden (0: hoy, 1: ayer)")
    registro: Optional[str] = Field(default=None, description="Registro")

    # DataX
    datax_name: Optional[str] = Field(default=None, description="Nombre DataX")
    datax_namespace: Optional[str] = Field(default=None, description="Namespace DataX")
    datax_source_params: Optional[List[ComponentParam]] = Field(default=None, description="Parámetros de origen DataX")
    datax_destination_params: Optional[List[ComponentParam]] = Field(default=None, description="Parámetros de destino DataX")

    # Parámetros comunes para Kirby y Hammurabi
    component_params: Optional[List[ComponentParam]] = Field(default=None, description="Parámetros para todos los componentes Kirby y Hammurabi")

    # Hammurabi
    hammurabi_staging: Optional[str] = Field(default=None, description="Hammurabi staging")
    hammurabi_raw: Optional[str] = Field(default=None, description="Hammurabi raw")
    hammurabi_master: Optional[str] = Field(default=None, description="Hammurabi master")
    hammurabi_l1t: Optional[str] = Field(default=None, description="Hammurabi L1T (solo si L2)")

    # Kirby
    kirby_raw: Optional[str] = Field(default=None, description="Kirby raw")
    kirby_master: Optional[str] = Field(default=None, description="Kirby master")
    kirby_l1t: Optional[str] = Field(default=None, description="Kirby L1T (solo si L2)")

    # Otros
    components_namespace: Optional[str] = Field(default=None, description="Namespace de los componentes")
    is_habile: Optional[bool] = Field(default=None, description="Si es día hábil")
    days: Optional[str] = Field(default=None, description="Días del mes (D1,D2... o 1,2...)")
    input_transmitted: Optional[str] = Field(default=None, description="Insumo transmitido a staging")
    table_name_raw: Optional[str] = Field(default=None, description="Nombre tabla raw")
    table_name_master: Optional[str] = Field(default=None, description="Nombre tabla master")
    email_cc_error: Optional[str] = Field(default=None, description="Emails CC para errores")
    user_story: Optional[str] = Field(default=None, description="Identificador de historia de usuario (ej: DEDFTRANSV-10074)")

def extractor(state: State):
    """Extrae información de la malla desde el historial de mensajes."""
    llm = init_chat_model("openai:gpt-4o-mini", temperature=0)
    llm_structured = llm.with_structured_output(schema=MeshInformation)

    prompt = prompt_template.format()
    mesh_info = llm_structured.invoke([("system", prompt)] + state["messages"])

    # Filtrar None para no sobreescribir campos ya extraídos en turnos anteriores
    return {k: v for k, v in mesh_info.model_dump().items() if v is not None}
