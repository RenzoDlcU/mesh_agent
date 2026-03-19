"""
Nodo que sube el XML generado a Bitbucket creando un branch y un Pull Request.

Flujo:
    1. Crea un branch desde la rama por defecto
    2. Hace commit del XML vía git clone + push
    3. Crea un Pull Request para revisión
"""

from src.agents.mesh.state import State
from src.services.bitbucket import BitbucketServer


def bitbucket_writer(state: State) -> dict:
    """
    Sube el XML generado a Bitbucket y crea un Pull Request.

    La ruta del archivo sigue la convención:
        {scope}/{UUAA}/{parent_folder}.xml

    Ejemplo:
        Local/PPAD/CR-PEPADMEN-T02.xml
    """
    xml = state.get("control_m_xml")
    if not xml:
        print("✗ No hay XML generado para subir.")
        return {"pr_url": None}

    uuaa = state["uuaa"].upper()
    scope = state.get("scope", "Local")
    periodicity_code = "DIA" if state.get("periodicity", "").lower() == "diaria" else "MEN"
    parent_folder = state.get("parent_folder", f"CR-PE{uuaa[1:]}{periodicity_code}-T02")

    bb = BitbucketServer()

    if not bb.token:
        print("⚠ BITBUCKET_TOKEN vacío. Se omite el push a Bitbucket.")
        print("  El XML generado se mostrará en el chat.")
        return {"pr_url": None}

    user_story = state.get("user_story")
    if not user_story:
        print("✗ No se proporcionó historia de usuario para el nombre del branch.")
        return {"pr_url": None}

    branch_name = f"feature/{user_story}"

    file_path = f"{scope}/{uuaa}/{parent_folder}.xml"

    print(f"ℹ Configuración Bitbucket:")
    print(f"  URL base: {bb.base_url}")
    print(f"  Proyecto: {bb.project_key}")
    print(f"  Repo: {bb.repo_slug}")
    print(f"  Token presente: {'sí' if bb.token else 'NO'}")
    print(f"  Archivo destino: {file_path}")
    print(f"  Branch: {branch_name}")

    try:
        # 1. Obtener rama por defecto
        print("\n── Paso 1: Obtener rama por defecto ──")
        default_branch = bb.get_default_branch()
        print(f"  ✓ Rama por defecto: {default_branch}")
    except Exception as e:
        print(f"  ✗ FALLO en get_default_branch: {e}")
        return {"pr_url": f"Error en paso 1 (get_default_branch): {e}"}

    try:
        # 2. Crear branch (solo si no existe)
        print("\n── Paso 2: Crear/verificar branch ──")
        if bb.branch_exists(branch_name):
            print(f"  ℹ Branch ya existe, se reutiliza: {branch_name}")
        else:
            bb.create_branch(branch_name, default_branch)
            print(f"  ✓ Branch creado: {branch_name}")
    except Exception as e:
        print(f"  ✗ FALLO en create_branch: {e}")
        return {"pr_url": f"Error en paso 2 (create_branch): {e}"}

    try:
        # 3. Commit del XML (vía git clone + push)
        print("\n── Paso 3: Commit del XML (git push) ──")
        bb.commit_file(
            path=file_path,
            content=xml,
            message=f"{user_story} añadir malla Control-M {parent_folder}",
            branch=branch_name,
        )
        print(f"  ✓ XML commiteado en: {file_path}")
    except Exception as e:
        print(f"  ✗ FALLO en commit_file: {e}")
        return {"pr_url": f"Error en paso 3 (commit_file): {e}"}

    try:
        # 4. Crear Pull Request
        print("\n── Paso 4: Crear Pull Request ──")
        pr_data = bb.create_pull_request(
            title=f"[{uuaa}] Nueva malla Control-M: {parent_folder}",
            source_branch=branch_name,
            target_branch=default_branch,
            description=(
                f"Malla generada automáticamente por **DAIA Agent**.\n\n"
                f"| Campo | Valor |\n"
                f"|-------|-------|\n"
                f"| UUAA | {uuaa} |\n"
                f"| Scope | {scope} |\n"
                f"| Archivo | `{file_path}` |\n"
                f"| Periodicidad | {state.get('periodicity', 'N/A')} |\n"
                f"| Seguridad | {state.get('security_level', 'N/A')} |\n"
            ),
        )

        pr_url = bb.get_pr_url(pr_data)
        print(f"  ✓ Pull Request creado: {pr_url}")

        return {"pr_url": pr_url}

    except Exception as e:
        print(f"  ✗ FALLO en create_pull_request: {e}")
        return {"pr_url": f"Error en paso 4 (create_pull_request): {e}"}
