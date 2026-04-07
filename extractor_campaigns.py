import requests
from config import (
    META_ACCESS_TOKEN,
    META_AD_ACCOUNT_ID,
    META_API_VERSION,
)


def obtener_campaigns():
    if not META_ACCESS_TOKEN:
        raise ValueError("Falta META_ACCESS_TOKEN en el .env")

    if not META_AD_ACCOUNT_ID:
        raise ValueError("Falta META_AD_ACCOUNT_ID en el .env")

    url = f"https://graph.facebook.com/{META_API_VERSION}/{META_AD_ACCOUNT_ID}/campaigns"

    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "id,name,status,effective_status",
        "limit": 500
    }

    registros = []

    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        registros.extend(data.get("data", []))

        paging = data.get("paging", {})
        url = paging.get("next")
        params = None

    return registros
