"""
Generador de archivos de configuración Kirby (.conf) y Hammurabi (.json).

Genera 10 archivos a partir de los metadatos del datum y parámetros del usuario:

Archivos .conf:
    1. {raw_table}-inr-01.conf    → Kirby Raw (CSV staging → tabla raw)
    2. {raw_table}-qls-01.conf    → Hammurabi Staging (validación CSV)
    3. {raw_table}-qlr-01.conf    → Hammurabi Raw (validación tabla raw)
    4. {master_table}-inm-01.conf → Kirby Master (raw → master)
    5. {master_table}-qlm-01.conf → Hammurabi Master (validación master)

Archivos .json:
    6.  {uuaa}-pe-krb-inr-{short}p-01.json  → Job Kirby Raw
    7.  {uuaa}-pe-hmm-qlt-{short}s-01.json  → Job Hammurabi Staging
    8.  {uuaa}-pe-hmm-qlt-{short}r-01.json  → Job Hammurabi Raw
    9.  {uuaa}-pe-krb-inm-{short}p-01.json  → Job Kirby Master
    10. {uuaa}-pe-hmm-qlt-{short}m-01.json  → Job Hammurabi Master
"""

import json
import re

from src.agents.kirby_hammurabi.state import State


def _make_short_name(physical_name: str) -> str:
    """
    Genera nombre corto para jobName a partir del nombre físico de la tabla.
    t_kmux_murex_mk_vl_mov → murexmkvlmov
    t_pmkd_murex_mk_vl_mov → murexmkvlmov
    """
    # Quitar prefijo t_{system}_ y luego eliminar underscores
    parts = physical_name.split("_")
    # parts[0] = "t", parts[1] = system code, parts[2:] = nombre real
    if len(parts) >= 3:
        return "".join(parts[2:])
    return physical_name.replace("_", "")


# ---------------------------------------------------------------------------
# Generadores de .conf
# ---------------------------------------------------------------------------

def _gen_kirby_raw_conf(state: State) -> str:
    """Genera el .conf de Kirby Raw (inr): CSV staging → tabla raw."""
    source_path = state.get("source_path", "")
    schema_path = state.get("schema_path_raw", "")
    raw_table = state.get("raw_table", "")
    partitions = state.get("partitions", "cutoff_date")
    delimiter = state.get("source_delimiter", ";")
    charset = state.get("source_charset", "UTF-8")
    has_header = state.get("source_has_header", True)

    return f'''kirby {{
    input {{
        options {{
            delimiter="{delimiter}"
            header={str(has_header).lower()}
            castMode="notPermissive"
            charset="{charset}"
            includeMetadataAndDeleted=true
        }}
        paths=[
             "{source_path}"
        ]
        schema {{
            path = ${{ARTIFACTORY_UNIQUE_CACHE}}"/artifactory/"${{SCHEMAS_REPOSITORY}}"{schema_path}"
        }}
        type=csv
    }}
    output {{
        dropLeftoverFields = true
        mode="overwrite"
        force=true
        partition = ["{partitions}"]
        options {{
             partitionOverwriteMode = "dynamic"
        }}
        table = "{raw_table}"
        compact = ${{COMPACT_VALUE}}
        compactConfig {{
            forceTargetPathRemove = true
            partitionDiscovery = true
            report = true
        }}
        type = table
    }}
    transformations=[
        {{
          field = "{partitions}"
          type = "literal"
          default = ${{?DATE}}
          defaultType = "string"
        }},
        {{
          type = "setCurrentDate"
          field = "audtiminsert_date"
        }},
        {{
          type = "formatter"
          field = "audtiminsert_date"
          typeToCast = "string"
        }}
    ]
}}'''


def _gen_hammurabi_staging_conf(state: State) -> str:
    """Genera el .conf de Hammurabi Staging (qls): validación CSV."""
    source_path = state.get("source_path", "")
    physical_target = source_path.split("/")[-1] if source_path else ""
    schema_path = state.get("schema_path_raw", "")
    uuaa = state.get("uuaa", "").lower()
    delimiter = state.get("source_delimiter", ";")
    charset = state.get("source_charset", "UTF-8")

    return f'''hammurabi {{
    dataFrameInfo {{
        cutoffDate=${{?REPROCESS_DATE}}
        targetPathName="{source_path}"
        physicalTargetName="{physical_target}"
        uuaa="{uuaa}"
    }}

    input {{
        options {{
            delimiter="{delimiter}"
            header=true
            castMode="notPermissive"
            charset="{charset}"
        }}
        paths = [
            "{source_path}"
        ]
        schema {{
            path = ${{ARTIFACTORY_UNIQUE_CACHE}}"/artifactory/"${{SCHEMAS_REPOSITORY}}"{schema_path}"
        }}
        type = "csv"
    }}

    rules=[
        {{
            class="com.datio.hammurabi.rules.completeness.CompletenessRule"
            config {{
                isCritical=true
                withRefusals=false
                minThreshold=100
                targetThreshold=100
                acceptanceMin=100
                id = "28e0d32b37"
            }}
        }},
        {{
            class="com.datio.hammurabi.rules.consistence.DuplicateRule"
            config {{
                columns=["TODO_COLUMN_1", "TODO_COLUMN_2"]
                isCritical=true
                withRefusals=true
                acceptanceMin=100
                minThreshold=100
                targetThreshold=100
                id = "28e0d33723"
            }}
        }}
    ]
}}'''


def _gen_hammurabi_raw_conf(state: State) -> str:
    """Genera el .conf de Hammurabi Raw (qlr): validación tabla raw vs CSV."""
    raw_table = state.get("raw_table", "")
    raw_physical_name = state.get("raw_physical_name", "")
    uuaa = state.get("uuaa", "").lower()
    source_path = state.get("source_path", "")
    schema_path = state.get("schema_path_raw", "")
    delimiter = state.get("source_delimiter", ";")
    charset = state.get("source_charset", "UTF-8")
    partitions = state.get("partitions", "cutoff_date")

    return f'''hammurabi {{
    dataFrameInfo {{
        subset = "{partitions}='"${{?DATE}}"'"
        cutoffDate = ${{?REPROCESS_DATE}}
        targetPathName="{raw_table}"
        physicalTargetName="{raw_physical_name}"
        uuaa="{uuaa}"
    }}

    input {{
        tables = [
          "{raw_table}"
        ]
        type = table
    }}

    rules=[
        {{
            class="com.datio.hammurabi.rules.completeness.BasicPerimeterCompletenessRule"
            config {{
                isCritical=true
                withRefusals=false
                minThreshold=100
                targetThreshold=100
                acceptanceMin=100
                dataValues {{
                    options {{
                        castMode="notPermissive"
                        delimiter="{delimiter}"
                        header = true
                        charset = "{charset}"
                        includeMetadataAndDeleted = true
                    }}
                    paths=[
                        "{source_path}"
                    ]
                    schema {{
                        path=${{ARTIFACTORY_UNIQUE_CACHE}}"/artifactory/"${{SCHEMAS_REPOSITORY}}"{schema_path}"
                    }}
                    type="csv"
                }}
                id = "TODO_RULE_ID"
            }}
        }}
    ]
}}'''


def _build_rename_block(schema_fields: list[dict]) -> str:
    """Genera el bloque renamecolumns a partir de los campos del schema."""
    if not schema_fields:
        return '''        {
            type : "renamecolumns"
            columnsToRename : {
                "TODO_SOURCE_COL_1": "TODO_TARGET_COL_1",
                "TODO_SOURCE_COL_2": "TODO_TARGET_COL_2"
            }
        }'''
    lines = []
    for f in schema_fields:
        lines.append(f'                "{f["legacyName"]}": "{f["name"]}"')
    rename_entries = ",\n".join(lines)
    return f'''        {{
            type : "renamecolumns"
            columnsToRename : {{
{rename_entries}
            }}
        }}'''


def _build_trim_block(schema_fields: list[dict]) -> str:
    """Genera el bloque trim con todos los campos master (por nombre post-rename)."""
    if not schema_fields:
        return ""
    names = [f["name"] for f in schema_fields]
    return f'''        {{
          type = "trim"
          field = "{"|".join(names)}"
          trimType = "both"
          regex = true
        }}'''


def _build_type_transformations(schema_fields: list[dict]) -> list[str]:
    """Genera transformaciones de tipo (dateformatter, formatter decimal) desde el schema."""
    if not schema_fields:
        return []

    date_fields = [f["name"] for f in schema_fields if f["type"] == "date"]
    decimal_fields = [f["name"] for f in schema_fields if f["type"].startswith("decimal")]

    blocks = []
    if date_fields:
        blocks.append(f'''        {{
            field="{"|".join(date_fields)}"
            regex = true
            type = "dateformatter"
            castMode = "notPermissive"
            format = "d/MM/yyyy"
        }}''')
    if decimal_fields:
        # Tomar el tipo decimal del primer campo como representativo
        decimal_type = next(f["type"] for f in schema_fields if f["type"].startswith("decimal"))
        blocks.append(f'''        {{
            field="{"|".join(decimal_fields)}"
            regex = true
            type="formatter"
            typeToCast="{decimal_type}"
        }}''')
    return blocks

def _build_select_columns_block(schema_fields: list[dict], partitions: str) -> str:
    """Genera el bloque selectcolumns con todos los campos master + partitions + audit."""
    if not schema_fields:
        return f'''        {{
            type = "selectcolumns"
            columnsToSelect=[
                "TODO_COLUMN_1",
                "TODO_COLUMN_2",
                "{partitions}",
                "audtiminsert_date"
            ]
        }}'''
    entries = [f'                "{f["name"]}"' for f in schema_fields]
    entries.append(f'                "{partitions}"')
    entries.append('                "audtiminsert_date"')
    return '''        {
            type = "selectcolumns"
            columnsToSelect=[
''' + ",\n".join(entries) + '''
            ]
        }'''


def _gen_kirby_master_conf(state: State) -> str:
    """Genera el .conf de Kirby Master (inm): raw → master."""
    raw_table = state.get("raw_table", "")
    master_table = state.get("master_table", "")
    partitions = state.get("partitions", "cutoff_date")
    schema_fields = state.get("schema_fields") or []

    # Construir bloques de transformación
    rename_block = _build_rename_block(schema_fields)
    trim_block = _build_trim_block(schema_fields)
    type_blocks = _build_type_transformations(schema_fields)
    select_block = _build_select_columns_block(schema_fields, partitions)

    # Ensamblar transformaciones
    transformations = []
    transformations.append(f'''        {{
         type = "sqlFilter"
         filter = "{partitions}='"${{?DATE}}"'"
        }}''')
    transformations.append(rename_block)
    if trim_block:
        transformations.append(trim_block)
    transformations.extend(type_blocks)
    transformations.append(f'''        {{
            type = "literal"
            field = "{partitions}"
            default = ${{?DATE}}
            defaultType = "string"
        }}''')
    transformations.append(f'''        {{
            type = "dateformatter"
            field = "{partitions}"
            format = "yyyyMMdd"
        }}''')
    transformations.append('''        {
            type = "setCurrentDate"
            field = "audtiminsert_date"
        }''')
    transformations.append(select_block)

    transformations_str = ",\n".join(transformations)

    return f'''kirby {{
    input {{
        applyConversions = false
        tables = [
            "{raw_table}"
        ]
        type = "table"
    }}
    output {{
        mode = overwrite
        compact = ${{COMPACT_VALUE}}
        compactConfig {{
            forceTargetPathRemove = true
            report = true
            partitionsFilter = "{partitions}='"${{?DATE}}"'"
        }}
        force = true
        options {{
            partitionOverwriteMode = "dynamic"
            keepPermissions = "true"
        }}
        partition = [
            "{partitions}"
        ]
        table = "{master_table}"
        type = table
        dropLeftoverFields = true
    }}
    transformations=[
{transformations_str}
    ]
}}'''


def _gen_hammurabi_master_conf(state: State) -> str:
    """Genera el .conf de Hammurabi Master (qlm): validación master vs raw."""
    master_table = state.get("master_table", "")
    master_physical_name = state.get("master_physical_name", "")
    raw_table = state.get("raw_table", "")
    uuaa = state.get("uuaa", "").lower()
    partitions = state.get("partitions", "cutoff_date")

    return f'''hammurabi {{
    dataFrameInfo {{
        cutoffDate = ${{?REPROCESS_DATE}}
        targetPathName = "{master_table}"
        physicalTargetName = "{master_physical_name}"
        subset = "{partitions}='"${{?REPROCESS_DATE}}"'"
        uuaa = "{uuaa}"
    }}

    input {{
        options {{
            includeMetadataAndDeleted = true
        }}
        tables = [
            "{master_table}"
        ]
        type = "table"
    }}

    rules = [
        {{
            class = "com.datio.hammurabi.rules.completeness.ConditionalPerimeterCompletenessRule"
            config {{
                dataValuesSubset = "{partitions}='"${{?DATE}}"'"
                dataValues {{
                    tables = ["{raw_table}"]
                    options {{
                        includeMetadataAndDeleted = true
                    }}
                    type = "table"
                    applyConversions = false
                }}
                minThreshold = 100
                targetThreshold = 100
                acceptanceMin = 100
                isCritical = true
                withRefusals = false
                id = "2918070d81"
            }}
        }}
    ]
}}'''


# ---------------------------------------------------------------------------
# Generadores de .json (definiciones de jobs)
# ---------------------------------------------------------------------------

def _gen_job_json(
    job_name: str,
    runtime: str,
    namespace: str,
    config_name: str,
    version: str,
    size: str,
    concurrency: int,
    is_kirby: bool,
) -> str:
    """Genera un JSON de definición de job Kirby o Hammurabi."""
    uuaa = job_name.split("-")[0] if "-" in job_name else ""
    meta_config = f"pe:{uuaa}:{namespace}:{config_name}:{version}"

    job = {
        "jobName": job_name,
        "description": f"Job {job_name} created with DAIA Agent",
        "kind": "processing",
        "params": {
            "metaConfig": meta_config,
            "sparkHistoryEnabled": "false",
        },
        "runtime": runtime,
        "size": size,
        "streaming": False,
        "concurrency": concurrency,
        "tags": [],
    }

    if is_kirby:
        job["env"] = {"COMPACT_VALUE": "true"}

    return json.dumps(job, indent=2)


def generator(state: State):
    """Genera todos los archivos .conf y .json de Kirby/Hammurabi."""
    uuaa = state.get("uuaa", "").lower()
    raw_physical = state.get("raw_physical_name", "")
    master_physical = state.get("master_physical_name", "")
    namespace = state.get("namespace", "")
    size = state.get("job_size", "S")
    concurrency = state.get("concurrency", 49)
    ver_raw = state.get("metaconfig_version_raw", "0.1.0")
    ver_master = state.get("metaconfig_version_master", "0.1.0")

    short_name = _make_short_name(raw_physical)

    generated_files = []

    # --- 5 archivos .conf ---
    conf_files = [
        (f"{raw_physical}-inr-01.conf", _gen_kirby_raw_conf(state)),
        (f"{raw_physical}-qls-01.conf", _gen_hammurabi_staging_conf(state)),
        (f"{raw_physical}-qlr-01.conf", _gen_hammurabi_raw_conf(state)),
        (f"{master_physical}-inm-01.conf", _gen_kirby_master_conf(state)),
        (f"{master_physical}-qlm-01.conf", _gen_hammurabi_master_conf(state)),
    ]
    for name, content in conf_files:
        generated_files.append({"name": name, "content": content})

    # --- 5 archivos .json ---
    json_files = [
        # Kirby Raw
        (
            f"{uuaa}-pe-krb-inr-{short_name}p-01.json",
            _gen_job_json(
                job_name=f"{uuaa}-pe-krb-inr-{short_name}p-01",
                runtime="kirby3-lts",
                namespace=namespace,
                config_name=f"{raw_physical}-inr-01",
                version=ver_raw,
                size=size,
                concurrency=concurrency,
                is_kirby=True,
            ),
        ),
        # Hammurabi Staging
        (
            f"{uuaa}-pe-hmm-qlt-{short_name}s-01.json",
            _gen_job_json(
                job_name=f"{uuaa}-pe-hmm-qlt-{short_name}s-01",
                runtime="hammurabi-lts",
                namespace=namespace,
                config_name=f"{raw_physical}-qls-01",
                version=ver_raw,
                size=size,
                concurrency=concurrency,
                is_kirby=False,
            ),
        ),
        # Hammurabi Raw
        (
            f"{uuaa}-pe-hmm-qlt-{short_name}r-01.json",
            _gen_job_json(
                job_name=f"{uuaa}-pe-hmm-qlt-{short_name}r-01",
                runtime="hammurabi-lts",
                namespace=namespace,
                config_name=f"{raw_physical}-qlr-01",
                version=ver_raw,
                size=size,
                concurrency=concurrency,
                is_kirby=False,
            ),
        ),
        # Kirby Master
        (
            f"{uuaa}-pe-krb-inm-{short_name}p-01.json",
            _gen_job_json(
                job_name=f"{uuaa}-pe-krb-inm-{short_name}p-01",
                runtime="kirby3-lts",
                namespace=namespace,
                config_name=f"{master_physical}-inm-01",
                version=ver_master,
                size=size,
                concurrency=concurrency,
                is_kirby=True,
            ),
        ),
        # Hammurabi Master
        (
            f"{uuaa}-pe-hmm-qlt-{short_name}m-01.json",
            _gen_job_json(
                job_name=f"{uuaa}-pe-hmm-qlt-{short_name}m-01",
                runtime="hammurabi-lts",
                namespace=namespace,
                config_name=f"{master_physical}-qlm-01",
                version=ver_master,
                size=size,
                concurrency=concurrency,
                is_kirby=False,
            ),
        ),
    ]
    for name, content in json_files:
        generated_files.append({"name": name, "content": content})

    return {"generated_files": generated_files}
