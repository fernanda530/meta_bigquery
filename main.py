from extractor_meta import obtener_insights
from extractor_campaigns import obtener_campaigns
from transformaciones_base import transformar_base
from transformaciones_resultados import transformar_resultados
from transformaciones_campaigns import transformar_campaigns
from loader_bigquery import (
    cargar_tabla_base_bigquery,
    cargar_tabla_resultados_bigquery
)
from config import ACTUALIZAR_GOOGLE_SHEETS


def enriquecer_con_campaigns(df_principal, df_campaigns):
    if df_principal.empty:
        return df_principal

    if df_campaigns.empty:
        print("No se encontraron campañas para enriquecer datos.")
        return df_principal

    if "id_campania" not in df_principal.columns:
        print("El dataframe principal no tiene 'id_campania'.")
        return df_principal

    if "id_campania" not in df_campaigns.columns:
        print("El dataframe de campañas no tiene 'id_campania'.")
        return df_principal

    df = df_principal.copy()
    df_camp = df_campaigns.copy()

    df["id_campania"] = df["id_campania"].astype(str)
    df_camp["id_campania"] = df_camp["id_campania"].astype(str)

    # Evita duplicados de columnas al hacer merge
    df = df.drop(columns=["status", "effective_status", "estado_campania"], errors="ignore")

    columnas_campaign = ["id_campania", "status", "effective_status", "estado_campania"]
    columnas_campaign_existentes = [c for c in columnas_campaign if c in df_camp.columns]

    df = df.merge(
        df_camp[columnas_campaign_existentes],
        on="id_campania",
        how="left"
    )

    return df


def main():
    print("Iniciando extracción de Meta...")

    # 1) Insights
    registros = obtener_insights()
    print(f"Registros crudos insights extraídos: {len(registros)}")

    # 2) Campaigns
    registros_campaigns = obtener_campaigns()
    print(f"Registros de campañas extraídos: {len(registros_campaigns)}")

    # 3) Transformaciones
    df_base = transformar_base(registros)
    df_resultados = transformar_resultados(registros)
    df_campaigns = transformar_campaigns(registros_campaigns)

    print(f"Filas base antes de enriquecer: {len(df_base)}")
    print(f"Filas resultados antes de enriquecer: {len(df_resultados)}")
    print(f"Filas campaigns: {len(df_campaigns)}")

    # 4) Enriquecer con campaigns
    df_base = enriquecer_con_campaigns(df_base, df_campaigns)
    df_resultados = enriquecer_con_campaigns(df_resultados, df_campaigns)

    print("Columnas finales base:")
    print(df_base.columns.tolist())

    print("Columnas finales resultados:")
    print(df_resultados.columns.tolist())

    if not df_base.empty:
        print("Vista previa base:")
        print(df_base.head(3))

    if not df_resultados.empty:
        print("Vista previa resultados:")
        print(df_resultados.head(3))

    # 5) Carga a BigQuery
    print("Cargando tabla base a BigQuery...")
    resumen_base = cargar_tabla_base_bigquery(df_base, write_mode="append")

    print("Cargando tabla resultados a BigQuery...")
    resumen_resultados = cargar_tabla_resultados_bigquery(df_resultados, write_mode="append")

    print("\n" + "#" * 60)
    print("RESUMEN DE CARGA BIGQUERY")
    print("#" * 60)
    print(f"BASE -> total procesado: {resumen_base['total_df']}")
    print(f"BASE -> insertados: {resumen_base['insertados']}")
    print(f"BASE -> repetidos/no insertados: {resumen_base['repetidos']}")
    print(f"BASE -> modo: {resumen_base['modo']}")
    print("-" * 60)
    print(f"RESULTADOS -> total procesado: {resumen_resultados['total_df']}")
    print(f"RESULTADOS -> insertados: {resumen_resultados['insertados']}")
    print(f"RESULTADOS -> repetidos/no insertados: {resumen_resultados['repetidos']}")
    print(f"RESULTADOS -> modo: {resumen_resultados['modo']}")
    print("#" * 60)

    print("BigQuery actualizado correctamente.")

    # 6) Google Sheets
    if ACTUALIZAR_GOOGLE_SHEETS:
        print("Actualizando Google Sheets...")
        from sheets_writer import actualizar_google_sheets
        actualizar_google_sheets(df_base, df_resultados)
        print("Google Sheets actualizado correctamente.")
    else:
        print("ACTUALIZAR_GOOGLE_SHEETS=False → No se actualiza Google Sheets.")

    print("Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()
