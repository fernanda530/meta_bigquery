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


def _asegurar_hoja(spreadsheet: gspread.Spreadsheet, nombre_hoja: str, rows: int = 1000, cols: int = 30):
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

    ids_existentes = _obtener_ids_existentes(worksheet, id_column_name)
    df_nuevo = df_export[~df_export[id_column_name].isin(ids_existentes)].copy()

    if df_nuevo.empty:
        print(f"No hay registros nuevos para la hoja '{nombre_hoja}'.")
        return

    valores_nuevos: List[List] = df_nuevo.values.tolist()
    valores_actuales = worksheet.get_all_values()

    if not valores_actuales:
        worksheet.update("A1", [df_export.columns.tolist()])
        worksheet.append_rows(valores_nuevos, value_input_option="USER_ENTERED")
    else:
        worksheet.append_rows(valores_nuevos, value_input_option="USER_ENTERED")

    print(f"Se agregaron {len(df_nuevo)} filas nuevas en la hoja '{nombre_hoja}'.")


def actualizar_google_sheets(df_base: pd.DataFrame, df_resultados: pd.DataFrame):
    _append_dataframe_si_nuevo(df_base, "base", "id_base")
    _append_dataframe_si_nuevo(df_resultados, "resultados", "id_resultado")
