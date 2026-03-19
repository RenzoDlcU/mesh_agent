from typing import Optional
from langchain.chat_models import init_chat_model
from pydantic import BaseModel, Field

from src.agents.kirby_hammurabi.state import State
from src.agents.kirby_hammurabi.nodes.extractor.prompt import prompt_template


class KirbyHammurabiParams(BaseModel):
    """Parámetros adicionales para generar configuraciones Kirby/Hammurabi."""

    namespace: Optional[str] = Field(
        default=None,
        description="Namespace del repositorio de configs (ej: pe-de-cpdin-inq-pmkd0000)",
    )
    source_delimiter: Optional[str] = Field(
        default=None,
        description="Delimitador del CSV de origen (;, |, ,)",
    )
    source_charset: Optional[str] = Field(
        default=None,
        description="Charset del archivo de origen (ej: UTF-8)",
    )
    source_has_header: Optional[bool] = Field(
        default=None,
        description="Si el archivo de origen tiene cabecera",
    )
    job_size: Optional[str] = Field(
        default=None,
        description="Tamaño del job: S, M o L",
    )
    concurrency: Optional[int] = Field(
        default=None,
        description="Nivel de concurrencia del job",
    )
    metaconfig_version_raw: Optional[str] = Field(
        default=None,
        description="Versión del metaConfig para componentes raw",
    )
    metaconfig_version_master: Optional[str] = Field(
        default=None,
        description="Versión del metaConfig para componentes master",
    )


def extractor(state: State):
    """Extrae parámetros adicionales desde el historial de mensajes."""
    llm = init_chat_model("openai:gpt-4o-mini", temperature=0)
    llm_structured = llm.with_structured_output(schema=KirbyHammurabiParams)

    prompt = prompt_template.format()
    params = llm_structured.invoke([("system", prompt)] + state["messages"])

    return {k: v for k, v in params.model_dump().items() if v is not None}
