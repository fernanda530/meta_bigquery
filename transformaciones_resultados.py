import pandas as pd
from datetime import datetime


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def mapear_tipo_resultado(tipo):
    mapping = {
        "comment": "Comentarios",
        "landing_page_view": "Vistas de página de destino",
        "like": "Me gusta",
        "link_click": "Clics en enlace",
        "offsite_conversion.custom.2092478158171367": "Conversión personalizada",
        "offsite_conversion.fb_pixel_custom": "Conversión personalizada Pixel",
        "omni_landing_page_view": "Vistas de página de destino",
        "onsite_conversion.messaging_block": "Bloqueos de mensaje",
        "onsite_conversion.messaging_conversation_started_7d": "Conversaciones iniciadas",
        "onsite_conversion.messaging_first_reply": "Primeras respuestas",
        "onsite_conversion.messaging_user_depth_2_message_send": "Mensajes enviados",
        "onsite_conversion.messaging_user_depth_3_message_send": "Mensajes enviados profundos",
        "onsite_conversion.post_net_comment": "Comentarios netos",
        "onsite_conversion.post_net_like": "Me gusta netos",
        "onsite_conversion.post_net_save": "Guardados netos",
        "onsite_conversion.post_save": "Guardados",
        "onsite_conversion.total_messaging_connection": "Conexiones de mensajería",
        "page_engagement": "Interacción con la página",
        "post": "Publicaciones",
        "post_engagement": "Interacción con la publicación",
        "post_interaction_gross": "Interacción bruta con publicación",
        "post_interaction_net": "Interacción neta con publicación",
        "post_reaction": "Reacciones",
    }
    return mapping.get(tipo, tipo)


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


def construir_dict_costos(lista_costos):
    costos = {}

    if isinstance(lista_costos, list):
        for item in lista_costos:
            action_type = item.get("action_type")
            value = to_float(item.get("value"))

            if action_type:
                costos[action_type] = value

    return costos


def construir_id_base(fecha_inicio, id_campania, id_anuncio, edad, genero):
    return (
        str(fecha_inicio) + "|" +
        str(id_campania) + "|" +
        str(id_anuncio) + "|" +
        str(edad) + "|" +
        str(genero)
    )


def construir_id_resultado(id_base, tipo_resultado_tecnico):
    return str(id_base) + "|" + str(tipo_resultado_tecnico)


def transformar_resultados(registros):
    if not registros:
        return pd.DataFrame()

    df_base = pd.DataFrame(registros)

    if "date_start" in df_base.columns:
        df_base["date_start"] = pd.to_datetime(df_base["date_start"], errors="coerce")

    if "date_stop" in df_base.columns:
        df_base["date_stop"] = pd.to_datetime(df_base["date_stop"], errors="coerce")

    filas = []

    for _, row in df_base.iterrows():
        acciones = row.get("actions", [])
        costos_dict = construir_dict_costos(row.get("cost_per_action_type", []))

        estado_efectivo = row.get("effective_status")
        estado_configurado = row.get("status")

        if pd.notna(estado_efectivo):
            estado_campania = clasificar_estado_campania(estado_efectivo)
        elif pd.notna(estado_configurado):
            estado_campania = clasificar_estado_campania(estado_configurado)
        else:
            estado_campania = None

        id_base = construir_id_base(
            row.get("date_start"),
            row.get("campaign_id"),
            row.get("ad_id"),
            row.get("age"),
            row.get("gender"),
        )

        if isinstance(acciones, list) and acciones:
            for accion in acciones:
                tipo_tecnico = accion.get("action_type")
                resultados = to_float(accion.get("value"))
                costo = costos_dict.get(tipo_tecnico)

                id_resultado = construir_id_resultado(id_base, tipo_tecnico)

                filas.append({
                    "id_resultado": id_resultado,
                    "id_base": id_base,
                    "fecha_inicio": row.get("date_start"),
                    "fecha_fin": row.get("date_stop"),
                    "id_campania": row.get("campaign_id"),
                    "nombre_campania": row.get("campaign_name"),
                    "status": estado_configurado,
                    "effective_status": estado_efectivo,
                    "estado_campania": estado_campania,
                    "id_anuncio": row.get("ad_id"),
                    "nombre_anuncio": row.get("ad_name"),
                    "nombre_conjunto_anuncios": row.get("adset_name"),
                    "objetivo": row.get("objective"),
                    "edad": row.get("age"),
                    "genero": row.get("gender"),
                    "tipo_resultado_tecnico": tipo_tecnico,
                    "tipo_resultado": mapear_tipo_resultado(tipo_tecnico),
                    "resultados": resultados,
                    "costo_por_resultado": costo,
                    "load_timestamp": datetime.now(),
                })

    df_resultados = pd.DataFrame(filas)

    if df_resultados.empty:
        return pd.DataFrame(columns=[
            "id_resultado",
            "id_base",
            "fecha_inicio",
            "fecha_fin",
            "id_campania",
            "nombre_campania",
            "status",
            "effective_status",
            "estado_campania",
            "id_anuncio",
            "nombre_anuncio",
            "nombre_conjunto_anuncios",
            "objetivo",
            "edad",
            "genero",
            "tipo_resultado_tecnico",
            "tipo_resultado",
            "resultados",
            "costo_por_resultado",
            "load_timestamp",
        ])

    # Limpieza de tipos
    columnas_float = [
        "resultados",
        "costo_por_resultado"
    ]

    for col in columnas_float:
        if col in df_resultados.columns:
            df_resultados[col] = pd.to_numeric(df_resultados[col], errors="coerce").astype("float64")

    columnas_finales = [
        "id_resultado",
        "id_base",
        "fecha_inicio",
        "fecha_fin",
        "id_campania",
        "nombre_campania",
        "status",
        "effective_status",
        "estado_campania",
        "id_anuncio",
        "nombre_anuncio",
        "nombre_conjunto_anuncios",
        "objetivo",
        "edad",
        "genero",
        "tipo_resultado_tecnico",
        "tipo_resultado",
        "resultados",
        "costo_por_resultado",
        "load_timestamp",
    ]

    columnas_existentes = [c for c in columnas_finales if c in df_resultados.columns]
    return df_resultados[columnas_existentes]
