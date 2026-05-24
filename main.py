from extractor_meta import obtener_campanias, obtener_insights
from transformaciones_base import transformar_base
from transformaciones_campaigns import transformar_campaigns
from transformaciones_estatus_campania import transformar_estatus_campania
from transformaciones_resultados import transformar_resultados
from config import ACTUALIZAR_GOOGLE_SHEETS, MODO_PRUEBA, EXPORTAR_EXCEL_PREVIEW


def enriquecer_con_campaigns(df_principal, df_campaigns):
    if df_principal.empty:
        return df_principal

    if df_campaigns.empty:
        print("No se encontraron campanias para enriquecer datos.")
        return df_principal

    if "id_campania" not in df_principal.columns:
        print("El dataframe principal no tiene 'id_campania'.")
        return df_principal

    if "id_campania" not in df_campaigns.columns:
        print("El dataframe de campanias no tiene 'id_campania'.")
        return df_principal

    df = df_principal.copy()
    df_camp = df_campaigns.copy()

    df["id_campania"] = df["id_campania"].astype(str)
    df_camp["id_campania"] = df_camp["id_campania"].astype(str)

    df = df.drop(columns=["status", "effective_status", "estado_campania"], errors="ignore")

    columnas_campaign = ["id_campania", "status", "effective_status", "estado_campania"]
    columnas_campaign_existentes = [c for c in columnas_campaign if c in df_camp.columns]

    df = df.merge(
        df_camp[columnas_campaign_existentes],
        on="id_campania",
        how="left",
    )

    return df


def main():
    print("Iniciando extracción de Meta...")

    registros = obtener_insights()
    print(f"Registros extraídos desde Meta: {len(registros)}")

    registros_campanias = obtener_campanias()
    print(f"\nCampanias extraidas desde Meta: {len(registros_campanias)}")

    df_campaigns = transformar_campaigns(registros_campanias)

    # Tabla base
    df_base = transformar_base(registros)
    df_base = enriquecer_con_campaigns(df_base, df_campaigns)
    print(f"\nFilas tabla base: {len(df_base)}")
    print("Columnas tabla base:")
    print(df_base.columns.tolist())

    if not df_base.empty:
        print("\nVista previa tabla base:")
        print(df_base.head())

    #Tabla resultados
    df_resultados = transformar_resultados(registros)
    df_resultados = enriquecer_con_campaigns(df_resultados, df_campaigns)
    print(f"\nFilas tabla resultados: {len(df_resultados)}")
    print("Columnas tabla resultados:")
    print(df_resultados.columns.tolist())
 
    if not df_resultados.empty:
        print("\nVista previa tabla resultados:")
        print(df_resultados.head())

    # Exportar excels
    if EXPORTAR_EXCEL_PREVIEW:
        if not df_base.empty:
            df_base.to_excel("preview_meta_base.xlsx", index=False)
            print("\nSe generó preview_meta_base.xlsx")

        if not df_resultados.empty:
            df_resultados.to_excel("preview_meta_resultados.xlsx", index=False)
            print("Se generó preview_meta_resultados.xlsx")

    # Bigquery
    if MODO_PRUEBA:
        print("\nMODO_PRUEBA=True → No se subirán datos a BigQuery.")
    else:
        print("\nMODO_PRUEBA=False -> Subiendo tablas a BigQuery...")

        from loader_bigquery import (
            cargar_tabla_base_bigquery,
            cargar_tabla_estatus_campania_bigquery,
            cargar_tabla_resultados_bigquery,
        )

        if not df_base.empty:
            cargar_tabla_base_bigquery(df_base)

        if not df_resultados.empty:
            cargar_tabla_resultados_bigquery(df_resultados)

    if ACTUALIZAR_GOOGLE_SHEETS:
        print("\nActualizando Google Sheets...")
        from sheets_writer import actualizar_google_sheets
        actualizar_google_sheets(df_base, df_resultados)
        print("Google Sheets actualizado correctamente.")
    else:
        print("\nNo se actualiza Google Sheets.")

    df_estatus_campania = transformar_estatus_campania(registros_campanias)
    print(f"\nFilas tabla estatus campania: {len(df_estatus_campania)}")
    print("Columnas tabla estatus campania:")
    print(df_estatus_campania.columns.tolist())

    if not df_estatus_campania.empty:
        print("\nVista previa tabla estatus campania:")
        print(df_estatus_campania.head())

        if EXPORTAR_EXCEL_PREVIEW:
            df_estatus_campania.to_excel("preview_meta_estatus_campania.xlsx", index=False)
            print("Se genero preview_meta_estatus_campania.xlsx")

    if MODO_PRUEBA:
        print("\nMODO_PRUEBA=True -> No se subira estatus campania a BigQuery.")
    else:
        if not df_estatus_campania.empty:
            cargar_tabla_estatus_campania_bigquery(df_estatus_campania)

    print("\nProceso finalizado.")


if __name__ == "__main__":
    main()
