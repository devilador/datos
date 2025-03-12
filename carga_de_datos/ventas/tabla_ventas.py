import mysql.connector
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
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


def select_excel_file():
    # Abre una ventana para seleccionar un archivo Excel
    
    Tk().withdraw()
    filename = askopenfilename(
        filetypes=[("Archivos Excel" , "*.xlsx"), ("Todos los archivos", "*.*")],
        title="Seleccionar archivo Excel"
    )
    return filename

def insert_rows_one_by_one(filepath):
    
    #Inserta datos desde un archivo Excel fila por fila en la base de datos
   
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Leer archivo Excel
        data = pd.read_excel(filepath)

        # Consulta SQL para insertar los datos
        insert_query = """
            INSERT INTO ventas (venta_id, fecha, tienda_id, producto_id, cliente_id, cantidad_vendida, precio_unitario, estatus, total_venta)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for i, row in data.iterrows():
            try:
                # Insertar fila actual
                cursor.execute(insert_query, (
                    row["venta_id"], row["fecha"], row["tienda_id"], row["producto_id"],
                    row["cliente_id"], row["cantidad_vendida"], row["precio_unitario"],
                    row["estatus"], row["total_venta"]
                ))
                # Confirmar cambios despues de cada fila
                connection.commit()
                print(f"Fila {i+1} insertada: {row.to_dict()}")
            except Exception as e:
                print(f"Error al insertar la fila {i+1}: {e}")

    except mysql.connector.Error as e:
        print(f"Error en la conexión o inserción: {e}")

    finally:
        # Cerrar la conexion
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

def main():
    #Función principal para gestionar la selección del archivo y la inserción fila por fila
    try:
        # Seleccionar archivo Excel
        excel_filepath = select_excel_file()

        if not excel_filepath:
            print("No se seleccionó ningún archivo.")
            return

        # Insertar filas una por una
        insert_rows_one_by_one(excel_filepath)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
