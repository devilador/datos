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
        filetypes=[("Archivos Excel ", "*.xlsx"), ("Todos los archivos", "*.*")],
        title="Seleccionar archivoo Excel"
    )
    return filename

def insert_inventario_from_excel(filepath):
    
    # Inserta datos fila por fila desde un archivo Excel en la tabla `inventario`.
   
    try:
        # Conectar a la base de datos
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Leer archivo Excel
        
        data = pd.read_excel(filepath)

        # Consulta SQL para insertar los datos
        
        insert_query = """
            INSERT INTO inventario (movimiento_id, fecha, almacen_id, producto_id, tipo_movimiento, cantidad, estatus)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for i, row in data.iterrows():
            try:
                # Insertar fila actual
                
                cursor.execute(insert_query, (
                    row["movimiento_id"], row["fecha"], row["almacen_id"], row["producto_id"],
                    row["tipo_movimiento"], row["cantidad"], row["estatus"]
                ))
                # Confirmar cambios después de cada fila
                connection.commit()
                print(f"Fila {i+1} insertada: {row.to_dict()}")
            except Exception as e:
                print(f"Error al insertar la fila {i+1}: {e}")

    except mysql.connector.Error as e:
        print(f"Error en la conexioon o insercin {e}")

    finally:
       
        # Cerrar la conexión
       
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexipn cerrada")

def main():
    
    # Función principal para gestionar la selección del archivo y la inserción fila por fila.
    
    try:
        
        # Seleccionar archivo Excel
        
        excel_filepath = select_excel_file()

        if not excel_filepath:
            print(" No se seleccion ning'n archivo.")
            return

        # Insertar filas desde Excel en la tabla `inventario`
        
        insert_inventario_from_excel(excel_filepath)

    except Exception as e:
        print(f" Error : {e}")
        

if __name__ ==  "__main__":
    main()
