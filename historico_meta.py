from datetime import datetime, timedelta

from extractor_meta import obtener_insights
from transformaciones_base import transformar_base
from transformaciones_resultados import transformar_resultados
from loader_bigquery import (
    cargar_tabla_base_bigquery,
    cargar_tabla_resultados_bigquery
)
from config import ACTUALIZAR_GOOGLE_SHEETS


def generar_bloques_fecha(fecha_inicio_str, fecha_fin_str, dias_por_bloque=7):
    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

    actual = fecha_inicio

    while actual <= fecha_fin:
        fin_bloque = min(actual + timedelta(days=dias_por_bloque - 1), fecha_fin)
        yield actual.isoformat(), fin_bloque.isoformat()
        actual = fin_bloque + timedelta(days=1)


def main():
    # 🔥 AJUSTA AQUÍ TU HISTÓRICO
    FECHA_INICIO = "2026-01-01"
    FECHA_FIN = "2026-04-05"
    DIAS_POR_BLOQUE = 7

    print("===== INICIO HISTÓRICO META =====")
    print(f"Rango total: {FECHA_INICIO} a {FECHA_FIN}")
    print(f"Tamaño de bloque: {DIAS_POR_BLOQUE} días")
    print(f"ACTUALIZAR_GOOGLE_SHEETS: {ACTUALIZAR_GOOGLE_SHEETS}")

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
            registros = obtener_insights(fecha_inicio=since, fecha_fin=until)
            cantidad_registros = len(registros)
            total_registros_crudos += cantidad_registros

            print(f"Registros crudos extraídos: {cantidad_registros}")

            if not registros:
                bloques_sin_datos += 1
                print("Bloque sin datos. Se omite.")
                continue

            bloques_con_datos += 1

            df_base = transformar_base(registros)
            df_resultados = transformar_resultados(registros)

            filas_base = len(df_base)
            filas_resultados = len(df_resultados)

            total_filas_base += filas_base
            total_filas_resultados += filas_resultados

            print(f"Filas tabla base: {filas_base}")
            print(f"Filas tabla resultados: {filas_resultados}")

            # Detectar tipos de resultado
            if not df_resultados.empty and "tipo_resultado_tecnico" in df_resultados.columns:
                tipos_bloque = df_resultados["tipo_resultado_tecnico"].dropna().unique().tolist()
                tipos_detectados.update(tipos_bloque)

            # 🔥 CLAVE: TRUNCATE SOLO EN EL PRIMER BLOQUE
            if i == 1:
                write_mode = "truncate"
            else:
                write_mode = "append"

            print(f"Modo de carga BigQuery: {write_mode}")

            # 🚀 CARGA A BIGQUERY
            cargar_tabla_base_bigquery(df_base, write_mode=write_mode)
            cargar_tabla_resultados_bigquery(df_resultados, write_mode=write_mode)

            print("BigQuery actualizado correctamente.")

            # 📊 GOOGLE SHEETS
            if ACTUALIZAR_GOOGLE_SHEETS:
                print("Actualizando Google Sheets...")
                from sheets_writer import actualizar_google_sheets
                actualizar_google_sheets(df_base, df_resultados)
                print("Google Sheets actualizado correctamente.")
            else:
                print("No se actualiza Google Sheets.")

        except Exception as e:
            import traceback
            bloques_con_error += 1
            print(f"Error en el bloque {since} a {until}:")
            print(repr(e))
            traceback.print_exc()
            print("Se continúa con el siguiente bloque...")

    # 📊 RESUMEN
    print("\n" + "-" * 60)
    print("TIPOS DE RESULTADO DETECTADOS")
    print("-" * 60)

    if tipos_detectados:
        for t in sorted(tipos_detectados):
            print("-", t)
    else:
        print("No se detectaron tipos.")

    print("\n" + "#" * 70)
    print("RESUMEN FINAL HISTÓRICO")
    print("#" * 70)
    print(f"Rango procesado: {FECHA_INICIO} a {FECHA_FIN}")
    print(f"Bloques totales: {len(bloques)}")
    print(f"Bloques con datos: {bloques_con_datos}")
    print(f"Bloques sin datos: {bloques_sin_datos}")
    print(f"Bloques con error: {bloques_con_error}")
    print(f"Total registros crudos: {total_registros_crudos}")
    print(f"Total filas base: {total_filas_base}")
    print(f"Total filas resultados: {total_filas_resultados}")
    print("#" * 70)
    print("===== FIN HISTÓRICO META =====")


if __name__ == "__main__":
    main()
