import re
from datetime import datetime
from src.agents.mesh.state import State
from src.agents.mesh.nodes.generator.controlm_params import build_component_params, build_sentry_parm, build_datax_cmdline, build_datax_variables

def set_parent_folder(state: State) -> str:
    uuaa = state["uuaa"]
    periodicity = "DIA" if state["periodicity"].lower() == "diaria" else "MEN"
    return f"CR-PE{uuaa[1:].upper()}{periodicity.upper()}-T02"

def set_periodicity(state: State) -> str:
    """" Establece los parámetros de periodicidad para el job de Control M según el estado proporcionado."""
    frequency_params = ""
    is_habile = state.get("is_habile")
    days = state.get("days", "")
    if state["periodicity"].lower() == "diaria":
        frequency_params = 'DAYS="ALL"' if is_habile is None else 'DAYSCAL="PEHABILE"'
    if state["periodicity"].lower() == "mensual":
        frequency_params = f'DAYS="{days}"' if is_habile is None else f'DAYS="{days}" DAYSCAL="PEHABILE"'
    return frequency_params

def generate_jobs_name(uuaa: str, next_correlatives: dict = None) -> dict:
    """
    Genera los nombres de los jobs de Control M basados en la UUAA.

    Si next_correlatives es proporcionado (leído de Bitbucket), los números
    continúan desde el último correlativo existente.
    Si no, empieza desde 1 (comportamiento original).

    Args:
        uuaa: Unidad Aplicativa (4 letras)
        next_correlatives: Dict con siguiente número por tipo {"T": n, "V": n, "C": n, "D": n}
    """
    if next_correlatives is None:
        next_correlatives = {"T": 1, "V": 1, "C": 1, "D": 1}

    t = next_correlatives.get("T", 1)
    v = next_correlatives.get("V", 1)
    c = next_correlatives.get("C", 1)
    d = next_correlatives.get("D", 1)

    return {
        "datax": f"{uuaa.upper()}TP{t:04d}",
        "hammurabi_staging": f"{uuaa.upper()}VP{v:04d}",
        "hammurabi_raw": f"{uuaa.upper()}VP{v+1:04d}",
        "hammurabi_master": f"{uuaa.upper()}VP{v+2:04d}",
        "kirby_raw": f"{uuaa.upper()}CP{c:04d}",
        "kirby_master": f"{uuaa.upper()}CP{c+1:04d}",
        "kirby_l1t": f"{uuaa.upper()}CP{c+2:04d}",
        "hammurabi_l1t": f"{uuaa.upper()}VP{v+3:04d}",
        "hdfs": f"{uuaa.upper()}DP{d:04d}",
    }

def generate_control_m_xml(state: State) -> str:
    """
    Genera el XML de la malla de Control M con todos los componentes.

    El flujo de la malla es:
    DataX -> Hammurabi Staging -> Kirby Raw -> Hammurabi Raw -> Kirby Master -> Hammurabi Master
    """

    # Extraer valores del state con defaults
    uuaa = state["uuaa"]
    datax_name = state["datax_name"]
    datax_namespace = state["datax_namespace"]
    security_level = state.get("security_level", "")
    email_error = state["email_error"]
    email_cc_error = state.get("email_cc_error", "")
    # periodicity = state["periodicity"]
    # order_date = state.get("order_date", 0)
    execution_time = state.get("execution_time", "")
    registro = state.get("registro", "")
    datax_source_params = state.get("datax_source_params", [])
    datax_destination_params = state.get("datax_destination_params", [])
    component_params = state.get("component_params", [])
    hammurabi_staging = state["hammurabi_staging"]
    hammurabi_raw = state["hammurabi_raw"]
    hammurabi_master = state["hammurabi_master"]
    hammurabi_l1t = state.get("hammurabi_l1t", "")
    kirby_raw = state["kirby_raw"]
    kirby_master = state["kirby_master"]
    kirby_l1t = state.get("kirby_l1t", "")
    components_namespace = state["components_namespace"]

    next_correlatives = state.get("next_correlatives")
    jobs_name = generate_jobs_name(uuaa, next_correlatives)
    periodicity_param = set_periodicity(state)

    # Generar variables XML para componentes (Kirby y Hammurabi)
    component_variables_xml = build_component_params(component_params) if component_params else ""

    # Generar valor de SENTRY_PARM dinámicamente
    sentry_parm_value = build_sentry_parm(component_params) if component_params else '{{&quot;env&quot;:{{&quot;CONTROLM_JOB_ID&quot;:&quot;%%JOBNAME&quot;,&quot;CONTROLM_JOB_FLOW&quot;:&quot;%%SCHEDTAB&quot;}}}}'

    # Generar CMDLINE y variables de DataX
    datax_cmdline = build_datax_cmdline(datax_name, datax_namespace, datax_source_params, datax_destination_params)
    datax_variables_xml = build_datax_variables(datax_name, datax_namespace, datax_source_params, datax_destination_params)

    exported_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    creation_date = str(datetime.now().strftime("%Y%m%d"))
    creation_time = str(datetime.now().strftime("%H%M%S"))
    application = f"{uuaa[1:].upper()}-PE-ADA"
    parent_folder = state.get("parent_folder") or set_parent_folder(state)

    # Offset de JOBISN para modo append (agregar a malla existente)
    isn_offset = state.get("target_mesh_job_count") or 0

    data_x_job = f"""
        <JOB JOBISN="{isn_offset + 1}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-DATAX-CCR" JOBNAME="{jobs_name['datax']}" DESCRIPTION="TRANSMISIÓN DATAX - {state["input_transmitted"]}" CREATED_BY="{registro}" RUN_AS="epsilon-ctlm" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="datax-ctrlm" INTERVAL="00001M" CMDLINE="{datax_cmdline}" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" {periodicity_param} {f'TIMEFROM="{execution_time}"' if execution_time else ''} JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}"  RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" MULTY_AGENT="N" USE_INSTREAM_JCL="N" VERSION_OPCODE="N" IS_CURRENT_VERSION="Y" VERSION_SERIAL="3" VERSION_HOST="DESKTOP-G9ADR0B" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {datax_variables_xml}
            <OUTCOND NAME="{jobs_name["datax"]}-TO-{jobs_name["hammurabi_staging"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["hammurabi_staging"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - %%$ODATE" MESSAGE="0038Fallo en la transferencia del fichero." ATTACH_SYSOUT="D"/>
            </ON>
        </JOB>"""

    hammurabi_staging_job = f"""
        <JOB JOBISN="{isn_offset + 2}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-HAMMURABI-CCR" JOBNAME="{jobs_name['hammurabi_staging']}" DESCRIPTION="HAMMURABI - {state["input_transmitted"]}" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {hammurabi_staging} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["datax"]}-TO-{jobs_name["hammurabi_staging"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["datax"]}-TO-{jobs_name["hammurabi_staging"]}" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["hammurabi_staging"]}-TO-{jobs_name["kirby_raw"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["kirby_raw"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {hammurabi_staging} - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    kirby_raw_job = f"""
        <JOB JOBISN="{isn_offset + 3}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-RAW-CCR" JOBNAME="{jobs_name["kirby_raw"]}" DESCRIPTION="RAW - {state["table_name_raw"]}" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {kirby_raw} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["hammurabi_staging"]}-TO-{jobs_name["kirby_raw"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["hammurabi_staging"]}-TO-{jobs_name["kirby_raw"]}" ODATE="ODAT" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["kirby_raw"]}-TO-{jobs_name["hammurabi_raw"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["hammurabi_raw"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {kirby_raw} - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    hammurabi_raw_job = f"""
        <JOB JOBISN="{isn_offset + 4}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-RAW-CCR" JOBNAME="{jobs_name["hammurabi_raw"]}" DESCRIPTION="HAMMURABI RAW - {state["table_name_raw"]}" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {hammurabi_raw} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["kirby_raw"]}-TO-{jobs_name["hammurabi_raw"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["kirby_raw"]}-TO-{jobs_name["hammurabi_raw"]}" ODATE="ODAT" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["hammurabi_raw"]}-TO-{jobs_name["kirby_master"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["kirby_master"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {hammurabi_raw} - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    kirby_master_job = f"""
        <JOB JOBISN="{isn_offset + 5}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-MASTER-CCR" JOBNAME="{jobs_name["kirby_master"]}" DESCRIPTION="MASTER - {state["table_name_master"]}" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {kirby_master} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["hammurabi_raw"]}-TO-{jobs_name["kirby_master"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["hammurabi_raw"]}-TO-{jobs_name["kirby_master"]}" ODATE="ODAT" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["kirby_master"]}-TO-{jobs_name["hammurabi_master"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["hammurabi_master"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {kirby_master} - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    l1t_out_condition = f"""
            <OUTCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["kirby_l1t"]}" ODATE="ODAT" SIGN="+"/>""" if security_level.upper() == "L2" else ""

    hammurabi_master_job = f"""
        <JOB JOBISN="{isn_offset + 6}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-MASTER-CCR" JOBNAME="{jobs_name["hammurabi_master"]}" DESCRIPTION="HAMMURABI MASTER - {state["table_name_master"]}" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {hammurabi_master} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["kirby_master"]}-TO-{jobs_name["hammurabi_master"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["kirby_master"]}-TO-{jobs_name["hammurabi_master"]}" ODATE="ODAT" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["hdfs"]}" ODATE="ODAT" SIGN="+"/>{l1t_out_condition}
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["hdfs"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {hammurabi_master}- %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    kirby_l1t_job = ""
    hammurabi_l1t_job = ""
    if security_level == "L2":
        kirby_l1t_job = f"""
        <JOB JOBISN="{isn_offset + 8}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-MASTER-CCR" JOBNAME="{jobs_name["kirby_l1t"]}" DESCRIPTION="MASTER - {state["kirby_master"]}_l1t" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {kirby_l1t} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["kirby_l1t"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["kirby_l1t"]}" ODATE="ODAT" SIGN="-"/>
            <OUTCOND NAME="{jobs_name["kirby_l1t"]}-TO-{jobs_name["hammurabi_l1t"]}" ODATE="ODAT" SIGN="+"/>
            <ON STMT="*" CODE="OK">
                <DOFORCEJOB TABLE_NAME="{parent_folder}" NAME="{jobs_name["hammurabi_l1t"]}" ODATE="ODAT" REMOTE="N"/>
            </ON>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {kirby_l1t} - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

        hammurabi_l1t_job = f"""
        <JOB JOBISN="{isn_offset + 9}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-MASTER-CCR" JOBNAME="{jobs_name["hammurabi_master"]}" DESCRIPTION="HAMMURABI MASTER - {state["kirby_master"]}_l1t" CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {hammurabi_l1t} -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["kirby_l1t"]}-TO-{jobs_name["hammurabi_l1t"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["kirby_l1t"]}-TO-{jobs_name["hammurabi_l1t"]}" ODATE="ODAT" SIGN="-"/>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {hammurabi_l1t}- %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""

    hdfs_job = f"""
        <JOB JOBISN="{isn_offset + 7}" APPLICATION="{application}" SUB_APPLICATION="{uuaa[1:].upper()}-MASTER-CCR" JOBNAME="{jobs_name["hdfs"]}" DESCRIPTION="REMOVE (HDFS) - " CREATED_BY="{registro}" RUN_AS="sentry" CRITICAL="0" TASKTYPE="Command" CYCLIC="0" NODEID="PE-SENTRY-00" INTERVAL="00001M" CMDLINE="/opt/datio/sentry-pe-aws/dataproc_sentry.py %%SENTRY_JOB %%SENTRY_OPT '%%SENTRY_PARM'" CONFIRM="0" RETRO="0" MAXWAIT="3" MAXRERUN="0" AUTOARCH="1" MAXDAYS="0" MAXRUNS="0" JAN="1" FEB="1" MAR="1" APR="1" MAY="1" JUN="1" JUL="1" AUG="1" SEP="1" OCT="1" NOV="1" DEC="1" DAYS_AND_OR="O" SHIFT="Ignore Job" SHIFTNUM="+00" SYSDB="1" IND_CYCLIC="S" CREATION_USER="{registro}" CREATION_DATE="{creation_date}" CREATION_TIME="{creation_time}" RULE_BASED_CALENDAR_RELATIONSHIP="O" APPL_TYPE="OS" CM_VER="N/A" MULTY_AGENT="N" USE_INSTREAM_JCL="N" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C" PARENT_FOLDER="{parent_folder}">
            {component_variables_xml}
            <VARIABLE NAME="%%PARM2" VALUE="REMOVE"/>
            <VARIABLE NAME="%%PARM3" VALUE="/in/staging/datax/pmph/H_MP_MEDIOS_PAGO_T_*%%PARM1..*"/>
            <VARIABLE NAME="%%SENTRY_JOB" VALUE="-ns {components_namespace} -jn {uuaa.lower()}-pe-dfs-rmv-hdfs-01 -o %%ORDERID"/>
            <VARIABLE NAME="%%SENTRY_OPT" VALUE="-b"/>
            <VARIABLE NAME="%%SENTRY_PARM" VALUE="{sentry_parm_value}"/>
            <SHOUT WHEN="EXECTIME" TIME="&gt;060" URGENCY="R" DEST="EM" MESSAGE="Excedio el tiempo de ejecucion favor de alertar al aplicativo" DAYSOFFSET="0"/>
            <INCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["hdfs"]}" ODATE="ODAT" AND_OR="A"/>
            <QUANTITATIVE NAME="DATIO_SENTRY_PE" QUANT="1" ONFAIL="R" ONOK="R"/>
            <OUTCOND NAME="{jobs_name["hammurabi_master"]}-TO-{jobs_name["hdfs"]}" ODATE="ODAT" SIGN="-"/>
            <ON STMT="*" CODE="NOTOK">
                <DOMAIL URGENCY="R" DEST="{email_error}"{' CC_DEST="'+email_cc_error+'" ' if email_cc_error else " "}SUBJECT="Cancelado %%JOBNAME - {uuaa.lower()}-pe-dfs-rmv-hdfs-01 - %%$ODATE" ATTACH_SYSOUT="Y"/>
            </ON>
        </JOB>"""



    # Combinar todos los nuevos jobs
    all_new_jobs = (
        data_x_job
        + hammurabi_staging_job
        + kirby_raw_job
        + hammurabi_raw_job
        + kirby_master_job
        + hammurabi_master_job
        + hdfs_job
        + (kirby_l1t_job if security_level == "L2" else "")
        + (hammurabi_l1t_job if security_level == "L2" else "")
    )

    target_mesh_content = state.get("target_mesh_content")

    if target_mesh_content:
        # Modo append: insertar nuevos jobs en la malla existente
        # Actualizar el comentario <!--Exported on ...-->
        target_mesh_content = re.sub(
            r'<!--Exported on .+?-->',
            f'<!--Exported on {exported_time}-->',
            target_mesh_content
        )
        insert_pos = target_mesh_content.rfind("</FOLDER>")
        if insert_pos != -1:
            xml = (
                target_mesh_content[:insert_pos]
                + all_new_jobs + "\n        "
                + target_mesh_content[insert_pos:]
            )
        else:
            # Fallback: no se encontró </FOLDER>, generar XML completo
            xml = f"""<?xml version="1.0" encoding="utf-8"?>
    <!--Exported on {str(exported_time)}-->
    <DEFTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="Folder.xsd">
        <FOLDER DATACENTER="CTM_DATIOPROD" VERSION="921" PLATFORM="UNIX" FOLDER_NAME="{parent_folder}" MODIFIED="False" LAST_UPLOAD="20180618171138UTC" FOLDER_ORDER_METHOD="SYSTEM" REAL_FOLDER_ID="0" TYPE="1" USED_BY_CODE="0">
        {all_new_jobs}
        </FOLDER>
    </DEFTABLE>"""
    else:
        # Modo nuevo: generar XML completo con wrapper
        xml = f"""<?xml version="1.0" encoding="utf-8"?>
    <!--Exported on {str(exported_time)}-->
    <DEFTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="Folder.xsd">
        <FOLDER DATACENTER="CTM_DATIOPROD" VERSION="921" PLATFORM="UNIX" FOLDER_NAME="{parent_folder}" MODIFIED="False" LAST_UPLOAD="20180618171138UTC" FOLDER_ORDER_METHOD="SYSTEM" REAL_FOLDER_ID="0" TYPE="1" USED_BY_CODE="0">
        {all_new_jobs}
        </FOLDER>
    </DEFTABLE>"""
    return xml


def generator(state: State):
    """Genera el XML de la malla de Control M a partir de la información extraída."""

    xml = generate_control_m_xml(state)
    parent_folder = state.get("parent_folder") or set_parent_folder(state)

    # Guardar copia local (para debug)
    # filename = f"mesh_{state['uuaa']}_{state['datax_name']}.xml"
    # with open(filename, 'w', encoding='utf-8') as f:
    #     f.write(xml)
    # print(f"✓ XML guardado localmente: {filename}")

    return {"control_m_xml": xml, "parent_folder": parent_folder}
