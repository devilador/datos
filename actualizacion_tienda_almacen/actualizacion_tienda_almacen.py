import mysql.connector
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

# logging
logging.basicConfig(
    filename='tienda_almacen.log',  # Archivo de registro
    level=logging.INFO,             # Nivel de registro
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def actualizar_tienda_almacen():
    """
    Actualiza la tabla `tienda_almacen` con la información más reciente de tiendas y almacenes.
    """
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # 1. Consulta para obtener la información de tiendas y almacenes
        query_tienda_almacen = """
            SELECT 
                t.tienda_id,
                t.nombre_tienda,
                t.almacen_id,
                a.nombre_almacen,
                a.ubicacion
            FROM 
                tiendas t
            INNER JOIN 
                almacenes a ON t.almacen_id = a.almacen_id;
        """

        # Ejecutar la consulta
        cursor.execute(query_tienda_almacen)
        resultados = cursor.fetchall()

        # 2. Eliminar datos existentes en la tabla `tienda_almacen`
        cursor.execute("DELETE FROM tienda_almacen;")
        connection.commit()
        logging.info("Datos existentes en 'tienda_almacen' eliminados.")

        # 3. Insertar los nuevos resultados en la tabla `tienda_almacen`
        insert_query = """
            INSERT INTO tienda_almacen (tienda_id, nombre_tienda, almacen_id, nombre_almacen, ubicacion)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Contador de filas insertadas
        filas_insertadas = 0

        for fila in resultados:
            try:
                # Insertar fila actual
                cursor.execute(insert_query, fila)
                filas_insertadas += 1
            except mysql.connector.Error as e:
                logging.error(f"Error al insertar la fila {fila}: {e}")

        # Confirmar cambios en la base de datos
        connection.commit()
        logging.info(f"Se insertaron {filas_insertadas} filas en la tabla 'tienda_almacen'")

    except mysql.connector.Error as e:
        logging.error(f"Error en la conexión o consulta: {e}")

    except Exception as e:
        logging.error(f"Error inesperado: {e}")

    finally:
        # Cerrar la conexion
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("Conexin cerrada")

def main():
    #Función principal para actualizar la tabla tienda_almacen
    try:
        logging.info("Iniciando proceso de actualización de la tabla 'tienda_almacen'.")
        actualizar_tienda_almacen()
        logging.info("Proceso de actualización de la tabla 'tienda_almacen' finalizado.")

    except Exception as e:
        logging.error(f"Error en l funcin principal: {e}")

if __name__ == "__main__":
    main()