from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessage

from src.agents.mesh.state import State
from src.agents.mesh.nodes.file_parser.node import file_parser
from src.agents.mesh.nodes.extractor.node import extractor
from src.agents.mesh.nodes.conversation.node import conversation, get_missing_fields
from src.agents.mesh.nodes.bitbucket_reader.node import bitbucket_reader
from src.agents.mesh.nodes.generator.node import generator
from src.agents.mesh.nodes.bitbucket_writer.node import bitbucket_writer


def route_after_extraction(state: State) -> str:
    """Decide si la información está completa o falta algo."""
    missing = get_missing_fields(state)
    if missing:
        return "conversation"
    return "bitbucket_reader"


def respond_with_pr(state: State) -> dict:
    """Genera un mensaje final con el link del Pull Request o muestra el XML."""
    pr_url = state.get("pr_url", "")
    uuaa = state.get("uuaa", "")
    parent_folder = state.get("parent_folder", "")

    if pr_url and not pr_url.startswith("Error"):
        content = (
            f"✅ ¡Malla generada exitosamente!\n\n"
            f"**UUAA:** {uuaa}\n"
            f"**Folder:** {parent_folder}\n\n"
            f"🔗 **Pull Request:** {pr_url}\n\n"
            f"Revisa los cambios y aprueba el PR cuando estés listo."
        )
    elif not pr_url:
        # Sin token de Bitbucket → mostrar XML en el chat
        xml = state.get("control_m_xml", "")
        content = (
            f"✅ ¡Malla generada exitosamente!\n\n"
            f"**UUAA:** {uuaa}\n"
            f"**Folder:** {parent_folder}\n\n"
            f"⚠️ No se configuró `BITBUCKET_TOKEN`, por lo que no se creó PR.\n"
            f"Aquí tienes el XML generado:\n\n"
            f"```xml\n{xml}\n```"
        )
    else:
        content = f"❌ Hubo un problema al crear el PR: {pr_url}"

    return {"messages": [AIMessage(content=content)]}


builder = StateGraph(State)

builder.add_node("file_parser", file_parser)
builder.add_node("extractor", extractor)
builder.add_node("conversation", conversation)
builder.add_node("bitbucket_reader", bitbucket_reader)
builder.add_node("generator", generator)
builder.add_node("bitbucket_writer", bitbucket_writer)
builder.add_node("respond_with_pr", respond_with_pr)

builder.add_edge(START, "file_parser")
builder.add_edge("file_parser", "extractor")
builder.add_conditional_edges("extractor", route_after_extraction, {
    "conversation": "conversation",
    "bitbucket_reader": "bitbucket_reader",
})
builder.add_edge("conversation", "extractor")
builder.add_edge("bitbucket_reader", "generator")
builder.add_edge("generator", "bitbucket_writer")
builder.add_edge("bitbucket_writer", "respond_with_pr")
builder.add_edge("respond_with_pr", END)

memory = MemorySaver()
mesh_agent = builder.compile(checkpointer=memory, interrupt_after=["conversation"])
