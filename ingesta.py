import boto3
import psycopg2

def obtener_datos():
    conn = psycopg2.connect(dbname='prestamos_db', user='user', password='password', host='postgres_service_name')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM prestamos")
    datos = cursor.fetchall()
    cursor.close()
    conn.close()
    return datos

def cargar_a_s3(data):
    s3 = boto3.client('s3')
    with open('/tmp/datos_prestamos.csv', 'w') as f:
        for row in data:
            f.write(','.join(map(str, row)) + '\n')
    s3.upload_file('/tmp/datos_prestamos.csv', 'tu_bucket_s3', 'datos_prestamos.csv')

if __name__ == "__main__":
    datos = obtener_datos()
    cargar_a_s3(datos)
