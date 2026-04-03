from datetime import datetime, timedelta
from extractor_meta import obtener_insights
from transformaciones_base import transformar_base
from transformaciones_resultados import transformar_resultados
from loader_bigquery import (
    cargar_tabla_base_bigquery,
    cargar_tabla_resultados_bigquery,
)
from config import MODO_PRUEBA


def generar_bloques_fecha(fecha_inicio_str, fecha_fin_str, dias_por_bloque=7):
    """
    Genera bloques de fechas entre fecha_inicio y fecha_fin.
    Formato esperado: YYYY-MM-DD
    """
    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

    actual = fecha_inicio

    while actual <= fecha_fin:
        fin_bloque = min(actual + timedelta(days=dias_por_bloque - 1), fecha_fin)
        yield actual.isoformat(), fin_bloque.isoformat()
        actual = fin_bloque + timedelta(days=1)


def main():
    #Rango historico

    FECHA_INICIO = "2026-04-01"
    FECHA_FIN = "2026-04-02"
    DIAS_POR_BLOQUE = 1

    print("===== INICIO HISTÓRICO META =====")
    print(f"Rango total: {FECHA_INICIO} a {FECHA_FIN}")
    print(f"Tamaño de bloque: {DIAS_POR_BLOQUE} días")
    print(f"MODO_PRUEBA: {MODO_PRUEBA}")

    bloques = list(generar_bloques_fecha(FECHA_INICIO, FECHA_FIN, DIAS_POR_BLOQUE))
    print(f"Total de bloques a procesar: {len(bloques)}")

    total_registros_crudos = 0
    total_filas_base = 0
    total_filas_resultados = 0
    bloques_con_datos = 0
    bloques_sin_datos = 0
    bloques_con_error = 0

    tipos_detectados = set()

    for i, (since, until) in enumerate(bloques, start=1):
        print("\n" + "=" * 60)
        print(f"Bloque {i}/{len(bloques)} → {since} a {until}")
        print("=" * 60)

        try:
            
            # EXTRACCIÓN

            registros = obtener_insights(fecha_inicio=since, fecha_fin=until)
            cantidad_registros = len(registros)
            total_registros_crudos += cantidad_registros

            print(f"Registros crudos extraídos: {cantidad_registros}")

            if not registros:
                bloques_sin_datos += 1
                print("Bloque sin datos. Se omite.")
                continue

            bloques_con_datos += 1


            # TRANSFORMACIÓN

            df_base = transformar_base(registros)
            df_resultados = transformar_resultados(registros)

            filas_base = len(df_base)
            filas_resultados = len(df_resultados)

            total_filas_base += filas_base
            total_filas_resultados += filas_resultados

            print(f"Filas tabla base: {filas_base}")
            print(f"Filas tabla resultados: {filas_resultados}")

            if not df_base.empty:
                print("\nVista previa base:")
                print(df_base.head(2))

            if not df_resultados.empty:
                print("\nVista previa resultados:")
                print(df_resultados.head(2))


            # CARGA A BIGQUERY

            if MODO_PRUEBA:
                print("MODO_PRUEBA=True → No se suben datos a BigQuery en este bloque.")
            else:
                if not df_base.empty:
                    cargar_tabla_base_bigquery(df_base)

                if not df_resultados.empty:
                    cargar_tabla_resultados_bigquery(df_resultados)

                print(f"Bloque {since} a {until} cargado correctamente.")

        except Exception as e:
            import traceback
            bloques_con_error += 1
            print(f"Error en el bloque {since} a {until}:")
            print(repr(e))
            traceback.print_exc()
            print("Se continúa con el siguiente bloque...")


    # RESUMEN FINAL

    print("\n" + "#" * 70)
    print("RESUMEN FINAL HISTÓRICO")
    print("#" * 70)
    print(f"Rango procesado: {FECHA_INICIO} a {FECHA_FIN}")
    print(f"Bloques totales: {len(bloques)}")
    print(f"Bloques con datos: {bloques_con_datos}")
    print(f"Bloques sin datos: {bloques_sin_datos}")
    print(f"Bloques con error: {bloques_con_error}")
    print(f"Total registros crudos: {total_registros_crudos}")
    print(f"Total filas tabla base procesadas: {total_filas_base}")
    print(f"Total filas tabla resultados procesadas: {total_filas_resultados}")
    print("#" * 70)
    print("===== FIN HISTÓRICO META =====")


if __name__ == "__main__":
    main()