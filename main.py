from extractor_meta import obtener_insights
from transformaciones_base import transformar_base
from transformaciones_resultados import transformar_resultados
from config import MODO_PRUEBA, EXPORTAR_EXCEL_PREVIEW


def main():
    print("Iniciando extracción de Meta...")

    registros = obtener_insights()
    print(f"Registros extraídos desde Meta: {len(registros)}")

    # Tabla base
    df_base = transformar_base(registros)
    print(f"\nFilas tabla base: {len(df_base)}")
    print("Columnas tabla base:")
    print(df_base.columns.tolist())

    if not df_base.empty:
        print("\nVista previa tabla base:")
        print(df_base.head())

    #Tabla resultados
    df_resultados = transformar_resultados(registros)
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
        print("\nMODO_PRUEBA=False → Subiendo ambas tablas a BigQuery...")

        from loader_bigquery import (
            cargar_tabla_base_bigquery,
            cargar_tabla_resultados_bigquery,
        )

        if not df_base.empty:
            cargar_tabla_base_bigquery(df_base)

        if not df_resultados.empty:
            cargar_tabla_resultados_bigquery(df_resultados)

    print("\nProceso finalizado.")


if __name__ == "__main__":
    main()