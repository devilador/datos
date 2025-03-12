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
        filetypes=[("Archivos Excel", "*.xlsx"), ("Todos los archivos", "*.*")],
        title="Seleccionaer archivo Excel"
    )
    return filename

def insert_tiendas_from_excel(filepath):
   
    # Inserta datos fila por fila desde un archivo Excel en la tabla `tiendas`
    
    try:
        # Conectar a la base de datos
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Leer archivo Excel
        
        data = pd.read_excel(filepath)

        # Consulta SQL 
        
        insert_query = """
            INSERT INTO tiendas (tienda_id, nombre_tienda, almacen_id)
            VALUES (%s, %s, %s)
        """

        for i, row in data.iterrows():
            try:
                # Insertar fila actual
                cursor.execute(insert_query, (
                    row["tienda_id"], row["nombre_tienda"], row["almacen_id"]
                ))
                # Confirmar cambios despues de cada fila
                connection.commit()
                print(f"Fila {i+1} insertada: {row.to_dict()}")
            except Exception as e:
                print(f"Error al insertar la fila {i+1}: {e}")

    except mysql.connector.Error as e:
        print(f"Error en la conexin o inserción: {e}")

    finally:
        # Cerrar la conexion
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexion cerrada")

def main():
    
    #Funcion seleccipn del archivo y la insercion fila
    
    try:
        # Seleccionaer archivo Excel
        excel_filepath = select_excel_file()

        if not excel_filepath:
            print("No se seleccionp ningun archivo")
            return

        # Insertar filas desde Excel en la tabla `tiendas`
        insert_tiendas_from_excel(excel_filepath)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
