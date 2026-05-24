import json
from typing import List, Set

import gspread
import pandas as pd

from config import GOOGLE_SHEETS_SPREADSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON


def _obtener_cliente_gspread() -> gspread.Client:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Falta GOOGLE_SERVICE_ACCOUNT_JSON en variables de entorno")

    credenciales = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    client = gspread.service_account_from_dict(credenciales)
    return client


def _asegurar_hoja(
    spreadsheet: gspread.Spreadsheet,
    nombre_hoja: str,
    rows: int = 1000,
    cols: int = 30,
):
    try:
        worksheet = spreadsheet.worksheet(nombre_hoja)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=nombre_hoja, rows=rows, cols=cols)
    return worksheet


def _preparar_dataframe_para_sheets(df: pd.DataFrame) -> pd.DataFrame:
    df_export = df.copy()

    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = df_export[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_export = df_export.fillna("")
    return df_export


def _obtener_ids_existentes(worksheet, id_column_name: str) -> Set[str]:
    valores = worksheet.get_all_values()

    if not valores:
        return set()

    encabezados = valores[0]
    if id_column_name not in encabezados:
        return set()

    idx = encabezados.index(id_column_name)
    ids = set()

    for fila in valores[1:]:
        if len(fila) > idx and fila[idx]:
            ids.add(str(fila[idx]))

    return ids


def _reinicializar_hoja_con_encabezados(worksheet, columnas: List[str]):
    worksheet.clear()
    worksheet.update(range_name="A1", values=[columnas])


def _actualizar_encabezados_preservando_datos(
    worksheet,
    nombre_hoja: str,
    encabezados_actuales: List[str],
    columnas_nuevas: List[str],
) -> List[str]:
    columnas_faltantes = [
        columna for columna in columnas_nuevas
        if columna not in encabezados_actuales
    ]

    if not columnas_faltantes:
        return encabezados_actuales

    columnas_finales = encabezados_actuales + columnas_faltantes
    worksheet.update(range_name="A1", values=[columnas_finales])

    print(
        f"Se agregaron columnas nuevas en '{nombre_hoja}': "
        f"{columnas_faltantes}"
    )

    return columnas_finales


def _append_dataframe_si_nuevo(df: pd.DataFrame, nombre_hoja: str, id_column_name: str):
    if df.empty:
        print(f"No hay datos para enviar a la hoja '{nombre_hoja}'.")
        return

    if id_column_name not in df.columns:
        raise ValueError(f"No existe la columna '{id_column_name}' en el DataFrame.")

    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        raise ValueError("Falta GOOGLE_SHEETS_SPREADSHEET_ID en variables de entorno")

    client = _obtener_cliente_gspread()
    spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
    worksheet = _asegurar_hoja(spreadsheet, nombre_hoja)

    df_export = _preparar_dataframe_para_sheets(df)
    df_export[id_column_name] = df_export[id_column_name].astype(str)

    columnas_esperadas = df_export.columns.tolist()
    valores_actuales = worksheet.get_all_values()

    if not valores_actuales:
        print(f"La hoja '{nombre_hoja}' esta vacia. Se crearan encabezados.")
        _reinicializar_hoja_con_encabezados(worksheet, columnas_esperadas)
        columnas_salida = columnas_esperadas
        ids_existentes = set()
    else:
        encabezados_actuales = valores_actuales[0]

        if encabezados_actuales != columnas_esperadas:
            columnas_salida = _actualizar_encabezados_preservando_datos(
                worksheet,
                nombre_hoja,
                encabezados_actuales,
                columnas_esperadas,
            )
        else:
            columnas_salida = columnas_esperadas

        ids_existentes = _obtener_ids_existentes(worksheet, id_column_name)

    df_export = df_export.reindex(columns=columnas_salida, fill_value="")
    df_nuevo = df_export[~df_export[id_column_name].isin(ids_existentes)].copy()

    if df_nuevo.empty:
        print(f"No hay registros nuevos para la hoja '{nombre_hoja}'.")
        return

    valores_nuevos: List[List] = df_nuevo.values.tolist()
    worksheet.append_rows(valores_nuevos, value_input_option="USER_ENTERED")

    print(f"Se agregaron {len(df_nuevo)} filas nuevas en la hoja '{nombre_hoja}'.")


def _reemplazar_hoja_con_dataframe(df: pd.DataFrame, nombre_hoja: str):
    if df.empty:
        print(f"No hay datos para reemplazar la hoja '{nombre_hoja}'.")
        return

    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        raise ValueError("Falta GOOGLE_SHEETS_SPREADSHEET_ID en variables de entorno")

    client = _obtener_cliente_gspread()
    spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
    worksheet = _asegurar_hoja(spreadsheet, nombre_hoja)

    df_export = _preparar_dataframe_para_sheets(df)
    valores = [df_export.columns.tolist()] + df_export.values.tolist()

    worksheet.clear()
    worksheet.update(range_name="A1", values=valores)

    print(f"Se reemplazo la hoja '{nombre_hoja}' con {len(df_export)} filas.")


def actualizar_google_sheets(
    df_base: pd.DataFrame,
    df_resultados: pd.DataFrame,
    write_mode: str = "append",
):
    if write_mode == "truncate":
        _reemplazar_hoja_con_dataframe(df_base, "base")
        _reemplazar_hoja_con_dataframe(df_resultados, "resultados")
    elif write_mode == "append":
        _append_dataframe_si_nuevo(df_base, "base", "id_base")
        _append_dataframe_si_nuevo(df_resultados, "resultados", "id_resultado")
    else:
        raise ValueError("write_mode debe ser 'append' o 'truncate'")
