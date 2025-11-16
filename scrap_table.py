import requests
import boto3
import uuid
import datetime

def lambda_handler(event, context):
    # Endpoint JSON del IGP para los últimos 10 sismos (ordenados por fecha descendente)
    url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query?where=objectid>0&outFields=*&f=json&orderByFields=fecha DESC&resultRecordCount=10"
    
    # Realizar la solicitud HTTP al endpoint JSON
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder al endpoint de datos'
        }
    
    # Parsear el JSON
    data = response.json()
    if 'features' not in data or not data['features']:
        return {
            'statusCode': 404,
            'body': 'No se encontraron datos de sismos'
        }
    
    # Extraer las filas de los atributos de cada feature
    rows = []
    for feat in data['features']:
        attrs = feat['attributes']
        
        # Formatear fecha si es timestamp (en ms)
        fecha_raw = attrs.get('fecha')
        if isinstance(fecha_raw, (int, float)):
            dt = datetime.datetime.fromtimestamp(fecha_raw / 1000)
            fecha = dt.strftime('%Y/%m/%d')
        else:
            fecha = str(fecha_raw) if fecha_raw else ''
        
        row = {
            'ref': attrs.get('ref', ''),  # Código de reporte y descripción de ubicación
            'fecha': fecha,
            'hora': attrs.get('hora', ''),
            'magnitud': attrs.get('magnitud', ''),
            'prof': f"{attrs.get('prof', '')} km",  # Profundidad con unidad
            'lat': attrs.get('lat', ''),
            'lon': attrs.get('lon', ''),
            'departamento': attrs.get('departamento', '')
        }
        rows.append(row)
    
    # Conectar a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')
    
    # Eliminar todos los elementos existentes (limpiar tabla)
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'id': item['id']})
    
    # Insertar los nuevos datos
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        table.put_item(Item=row)
    
    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows  # Opcional: puedes serializar a JSON si es necesario, pero Lambda lo maneja
    }
