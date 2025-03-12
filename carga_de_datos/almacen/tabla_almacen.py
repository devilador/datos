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
    """Abre una ventana para seleccionar un archivo Excel (.xlsx)."""
    Tk().withdraw()
    filename = askopenfilename(
        filetypes=[("Archivos Excel", "*.xlsx"), ("Todos los archivos", "*.*")],
        title="Seleccionar archivo Excel"
    )
    return filename

def insert_almacenes_from_excel(filepath):
    """
    Inserta datos fila por fila desde un archivo Excel en la tabla `almacenes`.
    """
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Leer archivo Excel
        data = pd.read_excel(filepath)

        # Imprimir las columnas detectadas en el archivo Excel para verificar
        print("Columnas en el archivo:", data.columns)

        # Consulta SQL para insertar los datos
        insert_query = """
            INSERT INTO almacenes (almacen_id, nombre_almacen, ubicacion)
            VALUES (%s, %s, %s)
        """

        for i, row in data.iterrows():
            try:
                # Insertar fila actual
                cursor.execute(insert_query, (
                    row["almacen_id"], row["nombre_almacen"], row["ubicación"]
                ))
                # Confirmar cambios después de cada fila
                connection.commit()
                print(f"Fila {i+1} insertada: {row.to_dict()}")
            except Exception as e:
                print(f"Error al insertar la fila {i+1}: {e}")

    except mysql.connector.Error as e:
        print(f"Error en la conexión o inserción: {e}")

    finally:
        # Cerrar la conexión
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

def main():
    """Función principal para gestionar la selección del archivo y la inserción fila por fila."""
    try:
        # Seleccionar archivo Excel
        excel_filepath = select_excel_file()

        if not excel_filepath:
            print("No se seleccionó ningún archivo.")
            return

        # Insertar filas desde Excel en la tabla `almacenes`
        insert_almacenes_from_excel(excel_filepath)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
