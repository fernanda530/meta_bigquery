import requests
import json
from config import (
    META_ACCESS_TOKEN,
    META_AD_ACCOUNT_ID,
    META_API_VERSION,
    META_FIELDS,
    META_BREAKDOWNS,
    META_LEVEL,
    META_DATE_PRESET,
)


def obtener_insights(fecha_inicio=None, fecha_fin=None):
    if not META_ACCESS_TOKEN:
        raise ValueError("Falta META_ACCESS_TOKEN en el .env")

    if not META_AD_ACCOUNT_ID:
        raise ValueError("Falta META_AD_ACCOUNT_ID en el .env")

    url = f"https://graph.facebook.com/{META_API_VERSION}/{META_AD_ACCOUNT_ID}/insights"

    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": META_FIELDS,
        "breakdowns": META_BREAKDOWNS,
        "level": META_LEVEL,
        "limit": 500
    }

    if fecha_inicio and fecha_fin:
        params["time_range"] = json.dumps({
            "since": fecha_inicio,
            "until": fecha_fin
        })
    else:
        params["date_preset"] = META_DATE_PRESET

    registros = []

    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        registros.extend(data.get("data", []))

        # Después de la primera llamada, ya no se mandan params otra vez
        paging = data.get("paging", {})
        url = paging.get("next")
        params = None

    return registros