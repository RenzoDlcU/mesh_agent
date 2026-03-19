from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessage

from src.agents.kirby_hammurabi.state import State
from src.agents.kirby_hammurabi.nodes.file_parser.node import file_parser
from src.agents.kirby_hammurabi.nodes.extractor.node import extractor
from src.agents.kirby_hammurabi.nodes.conversation.node import (
    conversation,
    get_missing_fields,
)
from src.agents.kirby_hammurabi.nodes.generator.node import generator


def route_after_extraction(state: State) -> str:
    """Decide si la información está completa o falta algo."""
    missing = get_missing_fields(state)
    if missing:
        return "conversation"
    return "generator"


def respond_with_files(state: State) -> dict:
    """Genera un mensaje final con los archivos generados."""
    generated = state.get("generated_files", [])
    uuaa = state.get("uuaa", "")

    if not generated:
        content = "❌ No se pudieron generar los archivos de configuración."
        return {"messages": [AIMessage(content=content)]}

    conf_files = [f for f in generated if f["name"].endswith(".conf")]
    json_files = [f for f in generated if f["name"].endswith(".json")]

    parts = [
        f"✅ ¡Archivos generados exitosamente para **{uuaa}**!\n",
        f"Se generaron **{len(generated)} archivos** ({len(conf_files)} .conf + {len(json_files)} .json):\n",
    ]

    parts.append("**Configuraciones (.conf):**")
    for f in conf_files:
        parts.append(f"- `{f['name']}`")

    parts.append("\n**Jobs (.json):**")
    for f in json_files:
        parts.append(f"- `{f['name']}`")

    parts.append("\n---\n")

    # Mostrar contenido de cada archivo
    for f in generated:
        ext = "json" if f["name"].endswith(".json") else "hocon"
        parts.append(f"### 📄 {f['name']}")
        parts.append(f"```{ext}\n{f['content']}\n```\n")

    parts.append(
        "⚠️ Los campos marcados con `TODO_` deben completarse manualmente "
        "(IDs de reglas Hammurabi, mapeo de columnas para master, etc.)."
    )

    content = "\n".join(parts)
    return {"messages": [AIMessage(content=content)]}


builder = StateGraph(State)

builder.add_node("file_parser", file_parser)
builder.add_node("extractor", extractor)
builder.add_node("conversation", conversation)
builder.add_node("generator", generator)
builder.add_node("respond_with_files", respond_with_files)

builder.add_edge(START, "file_parser")
builder.add_edge("file_parser", "extractor")
builder.add_conditional_edges(
    "extractor",
    route_after_extraction,
    {
        "conversation": "conversation",
        "generator": "generator",
    },
)
builder.add_edge("conversation", "extractor")
builder.add_edge("generator", "respond_with_files")
builder.add_edge("respond_with_files", END)

memory = MemorySaver()
kirby_hammurabi_agent = builder.compile(
    checkpointer=memory, interrupt_after=["conversation"]
)
