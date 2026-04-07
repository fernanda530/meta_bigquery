import pandas as pd
from datetime import datetime


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


def transformar_campaigns(registros):
    if not registros:
        return pd.DataFrame(columns=[
            "id_campania",
            "nombre_campania_campaign",
            "status",
            "effective_status",
            "estado_campania",
            "load_timestamp"
        ])

    df = pd.DataFrame(registros)

    df = df.rename(columns={
        "id": "id_campania",
        "name": "nombre_campania_campaign"
    })

    if "effective_status" in df.columns:
        df["estado_campania"] = df["effective_status"].apply(clasificar_estado_campania)
    elif "status" in df.columns:
        df["estado_campania"] = df["status"].apply(clasificar_estado_campania)
    else:
        df["estado_campania"] = None

    df["load_timestamp"] = datetime.now()

    columnas_finales = [
        "id_campania",
        "nombre_campania_campaign",
        "status",
        "effective_status",
        "estado_campania",
        "load_timestamp"
    ]

    columnas_existentes = [c for c in columnas_finales if c in df.columns]
    return df[columnas_existentes]
