import mysql.connector
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
def realizar_innerjoin_e_insertar():
    """
    Realiza un INNER JOIN entre las tablas `tiendas` y `almacenes`,
    y luego inserta el resultado en la tabla `almacen_tienda`.
    """
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # 1. Realizar el INNER JOIN
        query_innerjoin = """
            SELECT 
                t.tienda_id, 
                t.nombre_tienda, 
                t.almacen_id, 
                a.nombre_almacen, 
                a.ubicacion
            FROM 
                tiendas t
            INNER JOIN 
                almacenes a
            ON 
                t.almacen_id = a.almacen_id;
        """

        # Ejecutar la consulta del INNER JOIN
        cursor.execute(query_innerjoin)
        resultados = cursor.fetchall()

        # 2. Insertar los resultados en la tabla `almacen_tienda`
        insert_query = """
            INSERT INTO almacen_tienda (tienda_id, nombre_tienda, almacen_id, nombre_almacen, ubicacion)
            VALUES (%s, %s, %s, %s, %s)
        """

        for fila in resultados:
            try:
                # Insertar fila actual
                cursor.execute(insert_query, fila)
                # Confirmar cambios después de cada fila
                connection.commit()
                print(f"Fila insertada: {fila}")
            except Exception as e:
                print(f"Error al insertar la fila {fila}: {e}")

    except mysql.connector.Error as e:
        print(f"Error en la conexión o inserción: {e}")

    finally:
        # Cerrar la conexión
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

def main():
    """Función principal para gestionar la inserción de datos."""
    try:
        # Realizar el INNER JOIN e insertar los datos
        realizar_innerjoin_e_insertar()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()