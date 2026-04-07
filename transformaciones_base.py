import pandas as pd
from datetime import datetime


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extraer_clicks_enlace(actions):
    if not isinstance(actions, list):
        return None

    for item in actions:
        if item.get("action_type") in {"link_click", "inline_link_click"}:
            return to_float(item.get("value"))

    return None

def clasificar_estado_campania(estado):
    estado = str(estado).upper()

    if estado == "ACTIVE":
        return "Activa"
    elif estado == "PAUSED":
        return "Pausada"
    elif estado == "ARCHIVED":
        return "Archivada"
    elif estado in {"DELETED", "DISAPPROVED", "WITH_ISSUES"}:
        return "Inactiva"
    else:
        return "Inactiva"
    
def construir_id_base(fecha_inicio, id_campania, id_anuncio, edad, genero):
    return (
        str(fecha_inicio) + "|" +
        str(id_campania) + "|" +
        str(id_anuncio) + "|" +
        str(edad) + "|" +
        str(genero)
    )


def transformar_base(registros):
    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)

    # Fechas
    if "date_start" in df.columns:
        df["date_start"] = pd.to_datetime(df["date_start"], errors="coerce").dt.normalize()

    if "date_stop" in df.columns:
        df["date_stop"] = pd.to_datetime(df["date_stop"], errors="coerce").dt.normalize()

    # Métricas base
    df["alcance"] = df["reach"].apply(to_float) if "reach" in df.columns else None
    df["importe_gastado_mxn"] = df["spend"].apply(to_float) if "spend" in df.columns else None
    df["cpc_general"] = df["cpc"].apply(to_float) if "cpc" in df.columns else None

    if "actions" in df.columns:
        df["clics_enlace"] = df["actions"].apply(extraer_clicks_enlace)
    else:
        df["clics_enlace"] = None

    df["cpc_enlace"] = df.apply(
        lambda row: (
            row["importe_gastado_mxn"] / row["clics_enlace"]
            if row.get("importe_gastado_mxn") not in (None, 0)
            and row.get("clics_enlace") not in (None, 0)
            else None
        ),
        axis=1
    )

    # Limpieza de tipos numéricos
    columnas_float = [
        "alcance",
        "clics_enlace",
        "cpc_enlace",
        "importe_gastado_mxn"
    ]

    for col in columnas_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        # Estado de campaña
    if "effective_status" in df.columns:
        df["estado_campania"] = df["effective_status"].apply(clasificar_estado_campania)
    elif "status" in df.columns:
        df["estado_campania"] = df["status"].apply(clasificar_estado_campania)
    else:
        df["estado_campania"] = None
    
    # Timestamp de carga
    df["load_timestamp"] = datetime.now()

    # Renombramos

    df = df.rename(columns={
        "date_start": "fecha_inicio",
        "date_stop": "fecha_fin",
        "campaign_id": "id_campania",
        "campaign_name": "nombre_campania",
        "ad_id": "id_anuncio",
        "ad_name": "nombre_anuncio",
        "adset_name": "nombre_conjunto_anuncios",
        "objective": "objetivo",
        "age": "edad",
        "gender": "genero",
        "status": "status",
        "effective_status": "effective_status"
    })
    

    if "fecha_inicio" in df.columns:
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")

    if "fecha_fin" in df.columns:
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], errors="coerce")

    # ID base
    df["id_base"] = df.apply(
        lambda row: construir_id_base(
            row.get("fecha_inicio"),
            row.get("id_campania"),
            row.get("id_anuncio"),
            row.get("edad"),
            row.get("genero"),
        ),
        axis=1
    )

    columnas_finales = [
        "id_base",
        "fecha_inicio",
        "fecha_fin",
        "alcance",
        "id_campania",
        "nombre_campania",
        "status",
        "effective_status",
        "id_anuncio",
        "nombre_anuncio",
        "clics_enlace",
        "cpc_enlace",
        "importe_gastado_mxn",
        "nombre_conjunto_anuncios",
        "objetivo",
        "edad",
        "genero",
        "load_timestamp"
    ]

    columnas_existentes = [c for c in columnas_finales if c in df.columns]
    return df[columnas_existentes]
