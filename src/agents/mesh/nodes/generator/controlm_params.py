"""
Utilidades para traducir parámetros de componentes a variables de Control M.

El nombre del parámetro (name) puede ser cualquiera (ej: PROCESS_DATE, FECHA_CORTE, MI_VAR).
El patrón (pattern) determina cómo se calcula el valor en Control M.
"""


# Mapeo de patrones de cálculo a variables de Control M
# El "pattern" indica el TIPO de cálculo, no el nombre de la variable
CALC_PATTERNS = {
    # Fecha actual (hoy)
    "TODAY_YYYYMMDD": {
        "controlm": "%%$ODATE",
        "description": "Fecha actual en formato YYYYMMDD (ej: 20260128)"
    },
    "TODAY_YYYY-MM-DD": {
        "controlm": "%%$OYEAR-%%OMONTH-%%ODAY",
        "description": "Fecha actual en formato YYYY-MM-DD (ej: 2026-01-28)"
    },

    # Fecha anterior (ayer, -1 día)
    "YESTERDAY_YYYYMMDD": {
        "controlm": "%%$CALCDATE %%$ODATE -1",
        "description": "Ayer en formato YYYYMMDD"
    },
    "YESTERDAY_YYYY-MM-DD": {
        "controlm": "%%$CALCDATE %%$OYEAR-%%OMONTH-%%ODAY -1",
        "description": "Ayer en formato YYYY-MM-DD"
    },

    # Fecha siguiente (mañana, +1 día)
    "TOMORROW_YYYYMMDD": {
        "controlm": "%%$CALCDATE %%$ODATE +1",
        "description": "Mañana en formato YYYYMMDD"
    },

    # Último día del mes anterior
    "LAST_DAY_PREV_MONTH": {
        "controlm": "%%$CALCDATE %%$OYEAR.%%OMONTH.01 -1",
        "description": "Último día del mes anterior (ej: 2025-12-31)"
    },

    # Primer día del mes actual
    "FIRST_DAY_CURRENT_MONTH": {
        "controlm": "%%$OYEAR-%%OMONTH-01",
        "description": "Primer día del mes actual"
    },

    # Primer día del mes anterior
    "FIRST_DAY_PREV_MONTH": {
        "controlm": "%%$CALCDATE %%$OYEAR.%%OMONTH.01 -1M",
        "description": "Primer día del mes anterior"
    },

    # Componentes de fecha
    "YEAR": {
        "controlm": "%%$OYEAR",
        "description": "Año actual (YYYY)"
    },
    "MONTH": {
        "controlm": "%%OMONTH",
        "description": "Mes actual (MM)"
    },
    "DAY": {
        "controlm": "%%ODAY",
        "description": "Día actual (DD)"
    },

    # Valor fijo
    "FIXED": {
        "controlm": None,  # Se usa el value directamente
        "description": "Valor fijo que no cambia"
    },
}


def get_controlm_value(pattern: str, fixed_value: str = None) -> str:
    """
    Obtiene el valor de Control M basado en el patrón de cálculo.

    Args:
        pattern: Patrón de cálculo (ej: "TODAY_YYYYMMDD", "YESTERDAY_YYYY-MM-DD", "FIXED")
        fixed_value: Valor fijo si el patrón es "FIXED"

    Returns:
        Variable de Control M correspondiente
    """
    pattern_upper = pattern.upper() if pattern else ""

    if pattern_upper == "FIXED":
        return fixed_value or ""

    if pattern_upper in CALC_PATTERNS:
        return CALC_PATTERNS[pattern_upper]["controlm"]

    # Si no se reconoce el patrón, devolver el valor fijo
    return fixed_value or ""


def build_component_params(params: list[dict], start_index: int = 1) -> str:
    """
    Construye las variables XML de Control M a partir de los parámetros.

    Args:
        params: Lista de diccionarios con {name, value, pattern}
                - name: Nombre de la variable (libre, ej: "PROCESS_DATE", "MI_FECHA")
                - value: Valor fijo si pattern es "FIXED"
                - pattern: Tipo de cálculo (TODAY_YYYYMMDD, YESTERDAY_YYYY-MM-DD, FIXED, etc.)
        start_index: Índice inicial para numerar los PARM (default: 1)

    Returns:
        String con las variables XML
    """
    if not params:
        return ""

    result = []
    for i, param in enumerate(params, start_index):
        pattern = param.get("pattern", "FIXED")
        value = param.get("value", "")

        controlm_value = get_controlm_value(pattern, value)
        result.append(f'<VARIABLE NAME="%%PARM{i}" VALUE="{controlm_value}"/>')

    return "\n            ".join(result)


def build_sentry_parm(params: list[dict]) -> str:
    """
    Construye el valor de SENTRY_PARM dinámicamente basado en los parámetros.

    Los parámetros CONTROLM_JOB_ID y CONTROLM_JOB_FLOW siempre son fijos.
    Los demás se generan según los params recibidos.

    Args:
        params: Lista de diccionarios con {name, value, pattern}

    Returns:
        String con el valor de SENTRY_PARM formateado para XML
    """
    # Parámetros dinámicos basados en component_params
    env_parts = []
    for i, param in enumerate(params, 1):
        name = param.get("name", f"PARAM{i}")
        env_parts.append(f'&quot;{name}&quot;:&quot;%%PARM{i}&quot;')

    # Parámetros fijos que siempre van
    env_parts.append('&quot;CONTROLM_JOB_ID&quot;:&quot;%%JOBNAME&quot;')
    env_parts.append('&quot;CONTROLM_JOB_FLOW&quot;:&quot;%%SCHEDTAB&quot;')

    # Construir el JSON
    env_content = ",".join(env_parts)
    return f'{{{{&quot;env&quot;:{{{{{env_content}}}}}}}}}'


def build_datax_cmdline(datax_name: str, datax_namespace: str, source_params: list[dict], dest_params: list[dict]) -> str:
    """
    Construye el CMDLINE de DataX con los parámetros de origen y destino.

    Args:
        datax_name: Nombre del transfer de DataX
        datax_namespace: Namespace de DataX
        source_params: Lista de parámetros de origen [{name, value, pattern}, ...]
        dest_params: Lista de parámetros de destino [{name, value, pattern}, ...]

    Returns:
        String con el CMDLINE completo para DataX
    """
    cmdline_parts = [
        "datax-agent",
        "--transferId %%PARM1",
        "--namespace %%PARM2"
    ]

    # Calcular índice inicial para source params (después de transferId y namespace)
    parm_index = 3

    # Agregar srcParams
    for param in (source_params or []):
        name = param.get("name", "")
        cmdline_parts.append(f'--srcParam &quot;{name}:%%PARM{parm_index}&quot;')
        parm_index += 1

    # Agregar dstParams
    for param in (dest_params or []):
        name = param.get("name", "")
        cmdline_parts.append(f'--dstParam &quot;{name}:%%PARM{parm_index}&quot;')
        parm_index += 1

    return " ".join(cmdline_parts)


def build_datax_variables(datax_name: str, datax_namespace: str, source_params: list[dict], dest_params: list[dict]) -> str:
    """
    Construye las variables XML para el job de DataX.

    Args:
        datax_name: Nombre del transfer de DataX
        datax_namespace: Namespace de DataX
        source_params: Lista de parámetros de origen
        dest_params: Lista de parámetros de destino

    Returns:
        String con las variables XML
    """
    variables = [
        f'<VARIABLE NAME="%%PARM1" VALUE="{datax_name}"/>',
        f'<VARIABLE NAME="%%PARM2" VALUE="{datax_namespace}"/>'
    ]

    parm_index = 3

    # Agregar variables para source params
    for param in (source_params or []):
        pattern = param.get("pattern", "FIXED")
        value = param.get("value", "")
        controlm_value = get_controlm_value(pattern, value)
        variables.append(f'<VARIABLE NAME="%%PARM{parm_index}" VALUE="{controlm_value}"/>')
        parm_index += 1

    # Agregar variables para dest params
    for param in (dest_params or []):
        pattern = param.get("pattern", "FIXED")
        value = param.get("value", "")
        controlm_value = get_controlm_value(pattern, value)
        variables.append(f'<VARIABLE NAME="%%PARM{parm_index}" VALUE="{controlm_value}"/>')
        parm_index += 1

    return "\n            ".join(variables)


