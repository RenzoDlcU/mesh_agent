from langchain_core.prompts import PromptTemplate

template = """\
Extrae información para generar los archivos de configuración Kirby (.conf) y Hammurabi (.json) \
para un flujo de ingesta de datos en BBVA Perú.

El usuario proporcionará archivos .datum (metadatos de tablas), y tú debes extraer \
la información adicional que el usuario provea en la conversación.

Campos a extraer:
- namespace: Namespace del repositorio de configuraciones (ej: "pe-de-cpdin-inq-pmkd0000"). \
  Suele tener el formato pe-XX-XXXXX-XXX-UUAA0000.
- source_delimiter: Delimitador del archivo CSV de origen (ej: ";", ",", "|")
- source_charset: Charset del archivo de origen (ej: "UTF-8", "ISO-8859-1")
- source_has_header: Si el archivo de origen tiene cabecera (true/false)
- job_size: Tamaño del job en la plataforma (S, M, L)
- concurrency: Nivel de concurrencia del job (número entero, ej: 49)
- metaconfig_version_raw: Versión del metaConfig para componentes raw (ej: "0.1.2")
- metaconfig_version_master: Versión del metaConfig para componentes master (ej: "0.1.3")

REGLAS CRÍTICAS:
- Si el usuario NO menciona un campo, devuelve None. NUNCA inventes valores.
- Si el mensaje no contiene información relevante (ej: saludos), devuelve TODOS los campos como None.
- No uses placeholders como "XXXX", "ejemplo", "N/A" o similares.
- Solo extrae datos explícitos del mensaje del usuario.
"""

prompt_template = PromptTemplate.from_template(template)
