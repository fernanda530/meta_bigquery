from config import GCP_PROJECT_ID, BQ_DATASET
from google.cloud import bigquery


# Tablas destino
BQ_TABLE_BASE = "meta_ads_base"
BQ_TABLE_RESULTADOS = "meta_ads_resultados"

#Consulta en BigQuery los IDs que ya existen.
def obtener_ids_existentes(client, table_id, id_column, ids_a_revisar):

    if not ids_a_revisar:
        return set()

    chunk_size = 5000
    ids_existentes = set()
    ids_lista = list(ids_a_revisar)

    for i in range(0, len(ids_lista), chunk_size):
        bloque = ids_lista[i:i + chunk_size]
        ids_formateados = ", ".join([f"'{str(x)}'" for x in bloque])

        query = f"""
            SELECT {id_column}
            FROM `{table_id}`
            WHERE {id_column} IN ({ids_formateados})
        """

        query_job = client.query(query)
        resultados = query_job.result()

        for row in resultados:
            ids_existentes.add(row[id_column])

    return ids_existentes

# Filtra el DataFrame y deja solo registros nuevos.

def filtrar_nuevos_registros(df, client, table_id, id_column):

    if df.empty:
        return df

    if id_column not in df.columns:
        raise ValueError(f"No existe la columna '{id_column}' en el DataFrame.")

    ids_locales = set(df[id_column].dropna().astype(str).tolist())

    try:
        ids_existentes = obtener_ids_existentes(client, table_id, id_column, ids_locales)
        print(f"IDs existentes en BigQuery para {table_id}: {len(ids_existentes)}")
    except Exception as e:
        print(f"No se pudieron consultar IDs existentes en {table_id}.")
        print(f"Detalle: {e}")
        print("Se asume que la tabla no existe todavía o está vacía.")
        ids_existentes = set()

    df[id_column] = df[id_column].astype(str)
    df_nuevo = df[~df[id_column].isin(ids_existentes)].copy()

    print(f"Registros nuevos a insertar en {table_id}: {len(df_nuevo)}")
    return df_nuevo

#Carga dataframe a bigquery
#Con append insertamos solo registros nuevos, con truncate remplaza toda la tabla
def _cargar_dataframe(df, table_name, id_column, write_mode="append"):

    if df.empty:
        print(f"No hay datos para cargar en {table_name}.")
        return

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    if write_mode == "truncate":
        disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        df_cargar = df.copy()
        print(f"Modo truncate para {table_id}: se reemplazará la tabla.")
    elif write_mode == "append":
        disposition = bigquery.WriteDisposition.WRITE_APPEND
        df_cargar = filtrar_nuevos_registros(df.copy(), client, table_id, id_column)

        if df_cargar.empty:
            print(f"No hay registros nuevos para insertar en {table_id}.")
            return
    else:
        raise ValueError("write_mode debe ser 'append' o 'truncate'")

    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition
    )

    job = client.load_table_from_dataframe(df_cargar, table_id, job_config=job_config)
    job.result()

    print(f"Datos cargados correctamente en {table_id}")


def cargar_tabla_base_bigquery(df_base, write_mode="append"):
    _cargar_dataframe(df_base, BQ_TABLE_BASE, "id_base", write_mode=write_mode)


def cargar_tabla_resultados_bigquery(df_resultados, write_mode="append"):
    _cargar_dataframe(df_resultados, BQ_TABLE_RESULTADOS, "id_resultado", write_mode=write_mode)