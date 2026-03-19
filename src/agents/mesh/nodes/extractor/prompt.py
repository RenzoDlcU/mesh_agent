from langchain_core.prompts import PromptTemplate

template = """\
Extrae información para una malla de Control M (BBVA Perú).

Flujo: DataX → Hammurabi Staging → Kirby Raw → Hammurabi Raw → Kirby Master → Hammurabi Master

Campos generales:
- periodicity: "diaria", "semanal", "mensual"
- execution_time: Hora en formato HHMM (ej: "0300" para 3 AM)
- uuaa, security_level (L1, L2, L3), order_date (0 o 1), registro
- email_error, email_cc_error
- user_story: Identificador de la historia de usuario (ej: "DEDFTRANSV-10074", extraído de frases como "historia de usuario DEDFTRANSV-10074")
- datax_name, datax_namespace, components_namespace
- hammurabi_staging, hammurabi_raw, hammurabi_master, hammurabi_l1t
- kirby_raw, kirby_master, kirby_l1t
- input_transmitted, table_name_raw, table_name_master

Reglas para is_habile y days:
- is_habile = True si menciona "día hábil"
- is_habile = False si no especifica
- days: "D1,D2..." si hábil, "1,2..." si calendario

PARÁMETROS:
- datax_source_params: Parámetros de ORIGEN del DataX
- datax_destination_params: Parámetros de DESTINO del DataX  
- component_params: Parámetros para TODOS los Kirby y Hammurabi (son los mismos)

Cada parámetro tiene: name (nombre libre), value (para FIXED), pattern (tipo de cálculo)

Patrones disponibles:
- TODAY_YYYYMMDD: Fecha actual YYYYMMDD (ej: 20260128)
- TODAY_YYYY-MM-DD: Fecha actual YYYY-MM-DD (ej: 2026-01-28)
- YESTERDAY_YYYYMMDD: Ayer YYYYMMDD
- YESTERDAY_YYYY-MM-DD: Ayer YYYY-MM-DD
- TOMORROW_YYYYMMDD: Mañana YYYYMMDD
- LAST_DAY_PREV_MONTH: Último día del mes anterior
- FIRST_DAY_CURRENT_MONTH: Primer día del mes actual
- FIXED: Valor fijo (usar campo "value")

Ejemplos:
- "ODATE 20260128" → {{name: "ODATE", pattern: "TODAY_YYYYMMDD"}}
- "CUTOFF_DATE 2026-01-28" → {{name: "CUTOFF_DATE", pattern: "TODAY_YYYY-MM-DD"}}
- "PORT siempre sera 10095" → {{name: "PORT", value: "10095", pattern: "FIXED"}}

REGLAS CRÍTICAS:
- Si el usuario NO menciona un campo, devuelve None. NUNCA inventes valores.
- Si el mensaje no contiene información sobre mallas (ej: saludos, preguntas generales), devuelve TODOS los campos como None.
- No uses placeholders como "XXXX", "ejemplo", "N/A" o similares. Solo extrae datos explícitos del mensaje.
"""

prompt_template = PromptTemplate.from_template(template)