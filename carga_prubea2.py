import psycopg2
import schedule
import time
import datetime
import threading

# Configuración de la conexión a la base de datos transaccional
config_bd_trans = {
    'dbname': 'db_proyecto',
    'user': 'nico',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# Configuración de la conexión a la base de datos analítica
config_bd_analitica = {
    'dbname': 'db_analisis',
    'user': 'nico',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

def obtener_conexion_bd_trans():
    return psycopg2.connect(**config_bd_trans)

def obtener_conexion_bd_analitica():
    return psycopg2.connect(**config_bd_analitica)

def obtener_ultimos_ids_procesados():
    """Recuperar los últimos IDs procesados de las tablas analíticas."""
    conexion = obtener_conexion_bd_analitica()
    with conexion.cursor() as cursor:
        cursor.execute("SELECT COALESCE(MAX(id_cita), 0) FROM FactCitas;")
        ultimo_id_cita = cursor.fetchone()[0]
        
        cursor.execute("SELECT COALESCE(MAX(id_cliente), 0) FROM DimClientes;")
        ultimo_id_cliente = cursor.fetchone()[0]
        
        cursor.execute("SELECT COALESCE(MAX(id_emple), 0) FROM DimEmpleados;")
        ultimo_id_empleado = cursor.fetchone()[0]
        
        cursor.execute("SELECT COALESCE(MAX(id_serv), 0) FROM DimServicios;")
        ultimo_id_servicio = cursor.fetchone()[0]
    conexion.close()
    return ultimo_id_cita, ultimo_id_cliente, ultimo_id_empleado, ultimo_id_servicio

def extraer_y_cargar_datos():
    print(f"{datetime.datetime.now()}: Iniciando la extracción y carga de datos...")

    try:
        conexion_trans = obtener_conexion_bd_trans()
        conexion_anal = obtener_conexion_bd_analitica()

        ultimo_id_cita, ultimo_id_cliente, ultimo_id_empleado, ultimo_id_servicio = obtener_ultimos_ids_procesados()

        with conexion_trans.cursor() as cursor_trans, conexion_anal.cursor() as cursor_anal:
            # Extraer y cargar datos en FactCitas
            cursor_trans.execute("""
                SELECT c.id_cita, c.id_sede, c.id_emple, c.id_cliente, c.hora_inicio, c.hora_fin, c.fecha_cita, c.total, sp.comuna_pelu, cl.comuna_cliente
                FROM public.cita c
                JOIN public.sede_pelu sp ON c.id_sede = sp.id_sede
                JOIN public.clientes cl ON c.id_cliente = cl.id_cliente
                WHERE c.id_cita > %s;
            """, (ultimo_id_cita,))
            datos_fact_citas = cursor_trans.fetchall()
            cursor_anal.executemany("""
                INSERT INTO FactCitas (id_cita, id_sede, id_emple, id_cliente, hora_inicio, hora_fin, fecha_cita, total, comuna_pelu, comuna_cliente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, datos_fact_citas)

            # Extraer y cargar datos en DimClientes
            cursor_trans.execute("""
                SELECT id_cliente, nombre_cliente, apellido_cliente, direccion_cliente, comuna_cliente, region_cliente, sexo, fecha_nacimiento, rut_cliente
                FROM public.clientes
                WHERE id_cliente > %s;
            """, (ultimo_id_cliente,))
            datos_dim_clientes = cursor_trans.fetchall()
            cursor_anal.executemany("""
                INSERT INTO DimClientes (id_cliente, nombre_cliente, apellido_cliente, direccion_cliente, comuna_cliente, region_cliente, sexo, fecha_nacimiento, rut_cliente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, datos_dim_clientes)

            # Extraer y cargar datos en DimEmpleados
            cursor_trans.execute("""
                SELECT id_emple, nombre_emple, apellido_emple, direccion_emplea, comuna_emple, region_emplea, rut_emplea, cargo, sueldo, id_sede
                FROM public.empleados
                WHERE id_emple > %s;
            """, (ultimo_id_empleado,))
            datos_dim_empleados = cursor_trans.fetchall()
            cursor_anal.executemany("""
                INSERT INTO DimEmpleados (id_emple, nombre_emple, apellido_emple, direccion_emplea, comuna_emple, region_emplea, rut_emplea, cargo, sueldo, id_sede)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, datos_dim_empleados)

            # Extraer y cargar datos en DimServicios
            cursor_trans.execute("""
                SELECT id_serv, tipo_serv, precio_serv
                FROM public.servicio
                WHERE id_serv > %s;
            """, (ultimo_id_servicio,))
            datos_dim_servicios = cursor_trans.fetchall()
            cursor_anal.executemany("""
                INSERT INTO DimServicios (id_serv, tipo_serv, precio_serv)
                VALUES (%s, %s, %s);
            """, datos_dim_servicios)

            conexion_anal.commit()

        conexion_trans.close()
        conexion_anal.close()

        print(f"{datetime.datetime.now()}: Extracción y carga de datos completada.")

    except Exception as e:
        print(f"Error: {e}")

def cuenta_regresiva_proxima_carga():
    while True:
        ahora = datetime.datetime.now()
        proxima_ejecucion = schedule.next_run()
        tiempo_restante = proxima_ejecucion - ahora
        horas, resto = divmod(tiempo_restante.total_seconds(), 3600)
        minutos, segundos = divmod(resto, 60)
        print(f"Tiempo restante para la próxima carga de datos: {int(horas)}h {int(minutos)}m {int(segundos)}s", end='\r')
        time.sleep(1)

# Ejecutar la carga de datos inmediatamente
extraer_y_cargar_datos()

# Programar la carga de datos una vez a la semana a partir de ahora
schedule.every(7).days.do(extraer_y_cargar_datos)

# Iniciar la cuenta regresiva en un hilo separado
hilo_cuenta_regresiva = threading.Thread(target=cuenta_regresiva_proxima_carga)
hilo_cuenta_regresiva.start()

# Mantener el script en ejecución
while True:
    schedule.run_pending()
    time.sleep(1)