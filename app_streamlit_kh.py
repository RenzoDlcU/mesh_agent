import streamlit as st
from dotenv import load_dotenv
import uuid

# Cargar variables de entorno ANTES de importar langchain/openai
load_dotenv()

from langchain_core.messages import HumanMessage
from src.agents.kirby_hammurabi_agent import kirby_hammurabi_agent

st.set_page_config(page_title="DAIA Agent — Kirby & Hammurabi", page_icon="⚙️")
st.title("⚙️ DAIA Agent — Generador de Kirby & Hammurabi")

if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []

# Botón para iniciar nueva conversación
if st.sidebar.button("🔄 Nueva conversación"):
    st.session_state.thread_id = str(uuid.uuid4())
    st.session_state.messages = []
    st.session_state.pop("datum_files_sent", None)
    st.rerun()

# Upload de archivos datum y schema
uploaded_files = st.sidebar.file_uploader(
    "📁 Archivos Datum y Schema",
    type=["datum", "schema"],
    accept_multiple_files=True,
    help="Sube los archivos .datum (raw y master) y opcionalmente el .schema del master para auto-generar transformaciones.",
)

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Describe los archivos Kirby/Hammurabi que necesitas..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    config = {"configurable": {"thread_id": st.session_state.thread_id}}

    # Preparar input con archivos datum si aún no se han enviado
    input_state = {"messages": [HumanMessage(content=prompt)]}
    if uploaded_files and not st.session_state.get("datum_files_sent"):
        input_state["uploaded_files"] = [
            {"name": f.name, "content": f.read().decode("utf-8")}
            for f in uploaded_files
        ]
        st.session_state.datum_files_sent = True

    with st.spinner("Procesando..."):
        result = kirby_hammurabi_agent.invoke(input_state, config=config)

    response = result["messages"][-1].content

    st.session_state.messages.append({"role": "assistant", "content": response})
    with st.chat_message("assistant"):
        st.markdown(response)
