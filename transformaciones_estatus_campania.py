from datetime import date

import pandas as pd


def transformar_estatus_campania(registros):
    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)

    df = df.rename(columns={
        "id": "id_campania",
        "name": "nombre_campania",
    })

    df["fecha_extraccion"] = date.today()

    columnas_fecha = [
        "created_time",
        "start_time",
        "updated_time",
        "stop_time",
    ]

    fechas_normalizadas = {}
    for col in columnas_fecha:
        if col in df.columns:
            fechas_normalizadas[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
            df[col] = fechas_normalizadas[col].dt.date

    if "start_time" in fechas_normalizadas and "stop_time" in fechas_normalizadas:
        df["dias_configurados"] = (
            fechas_normalizadas["stop_time"] - fechas_normalizadas["start_time"]
        ).dt.days
        df["dias_configurados"] = df["dias_configurados"].astype("Int64")
    else:
        df["dias_configurados"] = pd.Series(dtype="Int64")

    columnas_finales = [
        "id_campania",
        "nombre_campania",
        "status",
        "effective_status",
        "created_time",
        "start_time",
        "updated_time",
        "stop_time",
        "fecha_extraccion",
        "dias_configurados",
    ]

    columnas_existentes = [c for c in columnas_finales if c in df.columns]
    return df[columnas_existentes]
