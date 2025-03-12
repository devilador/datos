import mysql.connector
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de conexión a la base de datos
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

def generar_ventas_generales():
    """
    Genera la tabla de ventas generales combinando información de las tablas
    `ventas`, `productos`, `clientes`, `tiendas` y `categorías`, y actualiza los precios unitarios.
    """
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # 1. Crear la tabla `ventas_generales` (si no existe)
        crear_tabla_query = """
            CREATE TABLE IF NOT EXISTS ventas_generales (
                venta_id INT,
                fecha DATE,
                tienda_id INT,
                nombre_tienda VARCHAR(100),
                cliente_id INT,
                nombre_cliente VARCHAR(100),
                tipo_cliente VARCHAR(50),
                producto_id INT,
                nombre_producto VARCHAR(100),
                categoria VARCHAR(50),
                cantidad_vendida INT,
                precio_unitario_actualizado DECIMAL(10, 2),
                total_venta DECIMAL(10, 2)
            );
        """
        cursor.execute(crear_tabla_query)
        connection.commit()
        print("Tabla 'ventas_generales' creada exitosamente.")

        # 2. Consulta para obtener información detallada de las ventas
        query_ventas_generales = """
            SELECT 
                v.venta_id,
                v.fecha,
                v.tienda_id,
                t.nombre_tienda,
                v.cliente_id,
                c.nombre_cliente,
                c.tipo_cliente,
                v.producto_id,
                p.nombre_producto,
                p.categoria,
                v.cantidad_vendida,
                p.precio_base AS precio_unitario_actualizado,
                (v.cantidad_vendida * p.precio_base) AS total_venta
            FROM 
                ventas v
            INNER JOIN 
                productos p ON v.producto_id = p.producto_id
            INNER JOIN 
                clientes c ON v.cliente_id = c.cliente_id
            INNER JOIN 
                tiendas t ON v.tienda_id = t.tienda_id;
        """

        # Ejecutar la consulta
        cursor.execute(query_ventas_generales)
        resultados = cursor.fetchall()

        # 3. Insertar los resultados en la tabla `ventas_generales`
        insert_query = """
            INSERT INTO ventas_generales (
                venta_id, fecha, tienda_id, nombre_tienda, cliente_id, nombre_cliente,
                tipo_cliente, producto_id, nombre_producto, categoria, cantidad_vendida,
                precio_unitario_actualizado, total_venta
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    """Función principal para generar la tabla de ventas generales."""
    try:
        # Generar la tabla de ventas generales
        generar_ventas_generales()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()