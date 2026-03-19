"""
Parser de correlativos de jobs Control-M desde archivos XML de mallas.

Los jobs siguen el patrón: {UUAA}{TIPO}P{NÚMERO}
Tipos:
    T = DataX (Transfer)
    V = Hammurabi (Validation)
    C = Kirby (Curation)
    D = HDFS (Delete/cleanup)
"""

import re
import xml.etree.ElementTree as ET


# Tipos de job reconocidos
JOB_TYPES = {"T", "V", "C", "D"}


def parse_correlatives_from_xml(xml_content: str, uuaa: str) -> dict[str, int]:
    """
    Parsea un XML de malla Control-M para encontrar el máximo correlativo
    por tipo de job para una UUAA.

    Args:
        xml_content: Contenido XML de la malla
        uuaa: Unidad Aplicativa (4 letras, ej: PPAD)

    Returns:
        Dict con el máximo número encontrado por tipo.
        Ej: {"T": 1, "V": 3, "C": 2, "D": 1}
    """
    counters: dict[str, int] = {t: 0 for t in JOB_TYPES}

    # Pattern: UUAA (4 chars) + tipo (1 char) + P + dígitos
    pattern = re.compile(
        rf"^{re.escape(uuaa.upper())}([{''.join(JOB_TYPES)}])P(\d+)$"
    )

    try:
        root = ET.fromstring(xml_content)
        for job in root.iter("JOB"):
            jobname = job.get("JOBNAME", "")
            match = pattern.match(jobname)
            if match:
                job_type = match.group(1)
                number = int(match.group(2))
                counters[job_type] = max(counters[job_type], number)
    except ET.ParseError:
        pass  # XML malformado, ignorar

    return counters


def aggregate_correlatives(
    correlatives_list: list[dict[str, int]],
) -> dict[str, int]:
    """
    Agrega correlativos de múltiples mallas, quedándose con el máximo de cada tipo.

    Args:
        correlatives_list: Lista de dicts de correlativos de cada malla

    Returns:
        Dict con el máximo global por tipo
    """
    result: dict[str, int] = {t: 0 for t in JOB_TYPES}

    for correlatives in correlatives_list:
        for job_type, count in correlatives.items():
            result[job_type] = max(result[job_type], count)

    return result


def next_correlatives(max_correlatives: dict[str, int]) -> dict[str, int]:
    """
    Calcula el siguiente correlativo disponible para cada tipo.

    Args:
        max_correlatives: Máximo correlativo actual por tipo

    Returns:
        Dict con el próximo número disponible por tipo (max + 1)
    """
    return {k: v + 1 for k, v in max_correlatives.items()}


def count_jobs_in_xml(xml_content: str) -> int:
    """
    Cuenta el número de etiquetas <JOB> en un XML de malla Control-M.

    Args:
        xml_content: Contenido XML de la malla

    Returns:
        Cantidad de jobs encontrados
    """
    try:
        root = ET.fromstring(xml_content)
        return len(list(root.iter("JOB")))
    except ET.ParseError:
        return 0


def parse_file_correlative(filename: str) -> int | None:
    """
    Extrae el número correlativo de un archivo de malla.

    Ejemplo:
        "CR-PEMOLDIA-T02.xml" → 2
        "CR-PEMOLDIA-T05.xml" → 5

    Args:
        filename: Nombre del archivo XML

    Returns:
        Número correlativo o None si no se pudo parsear
    """
    match = re.search(r'-T(\d+)\.xml$', filename)
    return int(match.group(1)) if match else None


def next_file_correlative(existing_numbers: list[int]) -> int:
    """
    Calcula el siguiente correlativo de archivo según convención.

    Convención:
        - Primera malla: T02
        - Segunda: T05
        - Siguientes: +1 desde el máximo (T06, T07, T08 ...)

    Args:
        existing_numbers: Lista de correlativos existentes (ej: [2, 5, 6])

    Returns:
        Siguiente correlativo disponible
    """
    if not existing_numbers:
        return 2
    max_num = max(existing_numbers)
    if max_num < 5:
        return 5
    return max_num + 1
