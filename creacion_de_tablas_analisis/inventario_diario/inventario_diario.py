import mysql.connector
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv


load_dotenv()


db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}


# Configuración del logging para seguimiento
logging.basicConfig(
    filename='log_inventario.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def obtener_fecha_anterior():
    """Calcula la fecha de ayer en formato YYYY-MM-DD"""
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def obtener_movimientos(cursor, fecha):
    """Consulta la base de datos para obtener las entradas y salidas de inventario"""
    query = """
        SELECT 
            producto_id,
            %s AS fecha,
            SUM(CASE WHEN tipo_movimiento = 'entrada' THEN cantidad ELSE 0 END) AS total_entradas,
            SUM(CASE WHEN tipo_movimiento = 'salida' THEN cantidad ELSE 0 END) AS total_salidas
        FROM 
            inventario
        WHERE 
            fecha = %s AND estatus = 'completado'
        GROUP BY 
            producto_id
    """
    cursor.execute(query, (fecha, fecha))
    return cursor.fetchall()

def insertar_inventario(cursor, datos):
    """Inserta los datos en la tabla inventario_diario"""
    query = """
        INSERT INTO inventario_diario (producto_id, fecha, total_entradas, total_salidas)
        VALUES (%s, %s, %s, %s)
    """
    filas = 0
    for fila in datos:
        try:
            cursor.execute(query, fila)
            filas += 1
        except mysql.connector.Error as e:
            logging.error(f"Fallo al insertar {fila}: {str(e)}")
    
    return filas  

def generar_inventario_diario():
    """Genera el inventario diario basado en los movimientos registrados"""
    fecha = obtener_fecha_anterior()
    try:
        conexion = mysql.connector.connect(**db_config)
        cursor = conexion.cursor()

        movimientos = obtener_movimientos(cursor, fecha)
        if movimientos:
            filas = insertar_inventario(cursor, movimientos)
            conexion.commit()  # Asegurar cambios en la BD
            logging.info(f"{filas} registros agregados en 'inventario_diario' para {fecha}")
        else:
            logging.info(f"No se encontraron registros para {fecha}")  

    except mysql.connector.Error as e:
        logging.error(f"Error en BD: {str(e)}")  
    except Exception as e:
        logging.error(f"Error desconocido: {e}")  

    finally:
        if 'conexion' in locals() and conexion.is_connected():  
            cursor.close()
            conexion.close()
            logging.info("Se cerró la conexión a la BD.")

def main():
    """Ejecuta el proceso de actualización del inventario"""
    logging.info("Arrancando actualización de inventario")
    generar_inventario_diario()
    logging.info("Finalizó el proceso.")

if __name__ == "__main__":
    main()
