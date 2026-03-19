"""
Nodo que lee las mallas existentes en Bitbucket para determinar
los próximos correlativos disponibles para los jobs de la UUAA
y la malla destino donde insertar los nuevos jobs.

Estructura esperada del repo:
    Local/{UUAA}/CR-PE{...}.xml
    Global/{UUAA}/CR-PE{...}.xml

Regla de capacidad:
    Cada malla soporta hasta 100 etiquetas <JOB>.
    Si una malla tiene menos de 100 jobs, los nuevos se agregan ahí.
    Si todas están llenas, se crea una nueva con el siguiente correlativo.
    Convención de correlativos de archivo: T02 → T05 → T06 → T07 ...
"""

from src.agents.mesh.state import State
from src.services.bitbucket import BitbucketServer
from src.services.correlative_parser import (
    parse_correlatives_from_xml,
    aggregate_correlatives,
    next_correlatives,
    count_jobs_in_xml,
    parse_file_correlative,
    next_file_correlative,
)

MAX_JOBS_PER_MESH = 100


def infer_scope(uuaa: str) -> str:
    """
    Infiere el scope (Local/Global) basándose en la convención de UUAA.

    - P prefix → Local (Perú local)
    - K prefix → Global (Global)
    """
    return "Global" if uuaa.upper().startswith("K") else "Local"


def bitbucket_reader(state: State) -> dict:
    """
    Lee todas las mallas XML existentes para la UUAA en Bitbucket,
    determina los próximos correlativos disponibles y busca la malla
    destino donde insertar los nuevos jobs.

    Lógica de selección de malla destino:
        1. Si una malla existente tiene < 100 jobs → se agregan ahí.
        2. Si todas tienen >= 100 jobs → se crea un nuevo XML con
           el siguiente correlativo de archivo disponible.
        3. Si no existen mallas previas → se crea la primera (T02).
    """
    uuaa = state["uuaa"].upper()
    scope = state.get("scope") or infer_scope(uuaa)
    periodicity = state.get("periodicity", "diaria")
    periodicity_code = "DIA" if periodicity.lower() == "diaria" else "MEN"

    bb = BitbucketServer()

    all_correlatives: list[dict[str, int]] = []
    file_correlative_numbers: list[int] = []

    target_mesh_file = None
    target_mesh_content = None
    target_mesh_job_count = None
    parent_folder = None

    if not bb.token:
        print("⚠ BITBUCKET_TOKEN vacío. Se usarán valores por defecto (sin lectura de repo).")
        parent_folder = f"CR-PE{uuaa[1:]}{periodicity_code}-T02"
        next_corr = next_correlatives({})
        print(f"✓ Próximos correlativos para {uuaa}: {next_corr}")
        print(f"✓ Parent folder: {parent_folder}")
        return {
            "scope": scope,
            "next_correlatives": next_corr,
            "parent_folder": parent_folder,
            "target_mesh_file": None,
            "target_mesh_content": None,
            "target_mesh_job_count": None,
        }

    try:
        # Listar todos los XML en la carpeta de la UUAA
        uuaa_path = f"{scope}/{uuaa}"
        files = bb.list_files(uuaa_path)

        # Filtrar solo XMLs que coincidan con la periodicidad
        prefix = f"CR-PE{uuaa[1:]}{periodicity_code}"
        xml_files = [f for f in files if f.endswith(".xml") and f.startswith(prefix)]

        if not xml_files:
            print(f"No se encontraron mallas existentes para {uuaa} en {uuaa_path}")
        else:
            print(f"Encontradas {len(xml_files)} malla(s) para {uuaa}: {xml_files}")

        for filename in xml_files:
            filepath = f"{uuaa_path}/{filename}"
            xml_content = bb.get_file_content(filepath)

            # Correlativos de jobs
            correlatives = parse_correlatives_from_xml(xml_content, uuaa)
            all_correlatives.append(correlatives)

            # Correlativo de archivo
            file_corr = parse_file_correlative(filename)
            if file_corr is not None:
                file_correlative_numbers.append(file_corr)

            # Contar jobs
            job_count = count_jobs_in_xml(xml_content)

            print(f"  → {filename}: {job_count} jobs, correlativos: {correlatives}")

            # Buscar primera malla con capacidad disponible
            if target_mesh_file is None and job_count < MAX_JOBS_PER_MESH:
                target_mesh_file = filepath
                target_mesh_content = xml_content
                target_mesh_job_count = job_count
                parent_folder = filename.replace(".xml", "")
                print(f"  ✓ Malla destino (con capacidad): {filename} ({job_count}/{MAX_JOBS_PER_MESH} jobs)")

        # Si todas las mallas están llenas, calcular nuevo correlativo
        if target_mesh_file is None and xml_files:
            new_corr = next_file_correlative(file_correlative_numbers)
            parent_folder = f"CR-PE{uuaa[1:]}{periodicity_code}-T{new_corr:02d}"
            print(f"✓ Todas las mallas están llenas. Nuevo archivo: {parent_folder}.xml")

    except Exception as e:
        print(f"⚠ No se pudo leer Bitbucket: {e}")
        print("  Usando correlativos desde 1 y creando nueva malla T02.")

    # Si no se determinó parent_folder (sin mallas previas o error)
    if parent_folder is None:
        parent_folder = f"CR-PE{uuaa[1:]}{periodicity_code}-T02"

    # Agregar y calcular siguientes correlativos de jobs
    max_corr = aggregate_correlatives(all_correlatives)
    next_corr = next_correlatives(max_corr)

    print(f"✓ Próximos correlativos para {uuaa}: {next_corr}")
    print(f"✓ Parent folder: {parent_folder}")

    return {
        "scope": scope,
        "next_correlatives": next_corr,
        "parent_folder": parent_folder,
        "target_mesh_file": target_mesh_file,
        "target_mesh_content": target_mesh_content,
        "target_mesh_job_count": target_mesh_job_count,
    }
