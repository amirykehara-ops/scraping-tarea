# scrap_table.py
import json
import uuid
import os
from datetime import datetime, timezone, timedelta

import requests
import boto3

# Endpoint ArcGIS del IGP (capa "SismosReportados")
ARCGIS_LAYER_QUERY = (
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"
)

DYNAMO_TABLE_NAME = os.getenv("DYNAMO_TABLE", "TablaWebScrapping")
# Si tu Lambda tiene otra zona horaria que quieres reflejar, cámbiala. Peru es -5:00
PERU_TZ = timezone(timedelta(hours=-5))


def epoch_ms_to_iso(ms):
    """Convierte milliseconds since epoch (ArcGIS Date) a ISO local (Perú)."""
    if ms is None:
        return None
    try:
        # ArcGIS suele devolver fechas en ms (UTC)
        dt_utc = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        dt_local = dt_utc.astimezone(PERU_TZ)
        return dt_local.isoformat()
    except Exception:
        return None


def fetch_latest_sismos(limit=10):
    """Consulta el feature layer y devuelve una lista de sismos (dict)."""
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": limit,
        "f": "json"
    }
    resp = requests.get(ARCGIS_LAYER_QUERY, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    sismos = []
    for feat in features:
        attrs = feat.get("attributes", {})

        # Campos conocidos según la capa: fecha (ms), hora (string), magnitud, lat, lon, prof, ref, departamento, intensidad/int_
        fecha_iso = epoch_ms_to_iso(attrs.get("fecha"))
        s = {
            "id": str(uuid.uuid4()),
            "fecha_iso": fecha_iso,
            "fecha_raw_ms": attrs.get("fecha"),
            "hora": attrs.get("hora"),
            "magnitud": attrs.get("magnitud"),
            "lat": attrs.get("lat"),
            "lon": attrs.get("lon"),
            "prof": attrs.get("prof"),  # profundidad (valor int)
            "profundidad": attrs.get("profundidad"),  # texto categorizado
            "ref": attrs.get("ref"),
            "intensidad": attrs.get("int_") or attrs.get("intensidad"),
            "departamento": attrs.get("departamento"),
            # añade otros campos que te interesen
        }
        sismos.append({k: ("" if v is None else str(v)) for k, v in s.items()})
    return sismos


def clear_dynamo_table(table):
    """Elimina todos los items de la tabla. Nota: scan puede paginar si hay muchos items."""
    scan = table.scan(ProjectionExpression="id")
    items = scan.get("Items", [])
    with table.batch_writer() as batch:
        for it in items:
            batch.delete_item(Key={"id": it["id"]})
    # Si hay paginación
    while "LastEvaluatedKey" in scan:
        scan = table.scan(
            ProjectionExpression="id",
            ExclusiveStartKey=scan["LastEvaluatedKey"]
        )
        items = scan.get("Items", [])
        with table.batch_writer() as batch:
            for it in items:
                batch.delete_item(Key={"id": it["id"]})


def save_to_dynamo(sismos):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    # Limpiar tabla (si lo deseas)
    clear_dynamo_table(table)

    # Insertar nuevos registros
    with table.batch_writer() as batch:
        index = 1
        for s in sismos:
            item = s.copy()
            item["orden"] = str(index)  # número como string para evitar conflictos tipo
            batch.put_item(Item=item)
            index += 1

    return len(sismos)


def lambda_handler(event, context):
    try:
        sismos = fetch_latest_sismos(limit=10)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error consultando IGP ArcGIS", "detail": str(e)})
        }

    try:
        saved = save_to_dynamo(sismos)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error guardando en DynamoDB", "detail": str(e)})
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "message": f"{saved} registros guardados en {DYNAMO_TABLE_NAME}",
            "items": sismos
        }, ensure_ascii=False)
    }


# Cuando pruebes localmente, puedes ejecutar fetch_latest_sismos()
if __name__ == "__main__":
    print(fetch_latest_sismos(5))
