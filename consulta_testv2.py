import psycopg2
import pandas as pd
import pyoo
import subprocess

# Configurar la conexión a PostgreSQL
conn = psycopg2.connect(
    dbname='db_analisis',
    user='nico',
    password='1234',
    host='localhost',
    port='5432'
)

def execute_query(query):
    """ Ejecuta una consulta SQL y devuelve un DataFrame pandas con los resultados """
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)

def save_to_csv(df, filename):
    """ Guarda un DataFrame pandas en un archivo CSV """
    df.to_csv(filename, index=False)

def convert_to_ods(csv_filename):
    """ Convierte un archivo CSV a formato ODS usando unoconv """
    ods_filename = csv_filename.replace('.csv', '.ods')
    subprocess.run(['unoconv', '-f', 'ods', csv_filename])
    return ods_filename

# Consulta 1: Horario con más citas durante el día por peluquería, identificando la comuna
query1 = """
SELECT DISTINCT ON (id_sede)
    id_sede,
    hora_inicio AS Horario_pick,
    comuna_pelu,
    COUNT(id_cita) AS num_citas
FROM
    FactCitas
GROUP BY
    id_sede,
    hora_inicio,
    comuna_pelu
ORDER BY
    id_sede,
    COUNT(id_cita) DESC;
"""
df1 = execute_query(query1)

# Consulta 2: Lista de clientes que gastan más dinero por peluquería, indicando comuna del cliente y de peluquería, además de cuanto gasto
query2 = """
SELECT 
    F.id_cliente,
    C.comuna_cliente,
    F.comuna_pelu,
    SUM(F.total) AS total_gasto
FROM 
    FactCitas F
JOIN 
    DimClientes C ON F.id_cliente = C.id_cliente
GROUP BY 
    F.id_cliente, 
    C.comuna_cliente, 
    F.comuna_pelu
ORDER BY 
    total_gasto DESC;
"""
df2 = execute_query(query2)

# Consulta 3: Lista de peluqueros que ha ganado más por mes durante el 2023, esto por peluquería
query3 = """
SELECT 
    E.id_emple,
    E.nombre_emple,
    F.comuna_pelu,
    EXTRACT(MONTH FROM F.fecha_cita) AS mes,
    SUM(F.total) AS total_ganado
FROM 
    FactCitas F
JOIN 
    DimEmpleados E ON F.id_emple = E.id_emple
WHERE 
    EXTRACT(YEAR FROM F.fecha_cita) = 2023
GROUP BY 
    E.id_emple, 
    E.nombre_emple, 
    F.comuna_pelu, 
    mes
ORDER BY 
    total_ganado DESC;
"""
df3 = execute_query(query3)

# Consulta 4: Lista de clientes hombres que se cortan el pelo y la barba
query4 = """
SELECT DISTINCT 
    C.id_cliente,
    C.nombre_cliente,
    C.apellido_cliente
FROM 
    FactCitas F
JOIN 
    DimClientes C ON F.id_cliente = C.id_cliente
JOIN 
    DimServicios S ON F.id_sede = S.id_serv 
WHERE 
    C.sexo = 'M' 
    AND S.id_serv IN (1, 3);
"""
df4 = execute_query(query4)

# Consulta 6: Identificar el horario más concurrido por peluquería durante el 2019 y 2020, desagregados por mes
query6 = """
SELECT 
    id_sede,
    año,
    mes,
    hora_inicio,
    num_citas
FROM (
    SELECT 
        id_sede,
        EXTRACT(YEAR FROM fecha_cita) AS año,
        EXTRACT(MONTH FROM fecha_cita) AS mes,
        hora_inicio,
        COUNT(id_cita) AS num_citas,
        ROW_NUMBER() OVER(PARTITION BY id_sede, EXTRACT(YEAR FROM fecha_cita), EXTRACT(MONTH FROM fecha_cita) ORDER BY COUNT(id_cita) DESC) AS rn
    FROM 
        FactCitas
    WHERE 
        EXTRACT(YEAR FROM fecha_cita) IN (2019, 2020)
    GROUP BY 
        id_sede, 
        EXTRACT(YEAR FROM fecha_cita), 
        EXTRACT(MONTH FROM fecha_cita), 
        hora_inicio
) AS subquery
WHERE 
    rn = 1
ORDER BY 
    id_sede, 
    año, 
    mes;
"""
df6 = execute_query(query6)

# Consulta 7: Identificar al cliente que ha tenido las citas más largas por peluquería, por mes
query7 = """
SELECT 
    id_cliente,
    id_sede,
    EXTRACT(MONTH FROM fecha_cita) AS mes,
    MAX(EXTRACT(EPOCH FROM (hora_fin - hora_inicio))) AS duracion_maxima
FROM 
    FactCitas
GROUP BY 
    id_cliente, 
    id_sede, 
    EXTRACT(MONTH FROM fecha_cita)
ORDER BY 
    id_sede, 
    mes;
"""
df7 = execute_query(query7)

# Guardar los resultados en archivos CSV
save_to_csv(df1, 'consulta1.csv')
save_to_csv(df2, 'consulta2.csv')
save_to_csv(df3, 'consulta3.csv')
save_to_csv(df4, 'consulta4.csv')
save_to_csv(df6, 'consulta6.csv')
save_to_csv(df7, 'consulta7.csv')

# Convertir archivos CSV a formato ODS
ods_file1 = convert_to_ods('consulta1.csv')
ods_file2 = convert_to_ods('consulta2.csv')
ods_file3 = convert_to_ods('consulta3.csv')
ods_file4 = convert_to_ods('consulta4.csv')
ods_file6 = convert_to_ods('consulta6.csv')
ods_file7 = convert_to_ods('consulta7.csv')

# Conexión a LibreOffice Calc
desktop = pyoo.Desktop()
doc = desktop.create_spreadsheet()

# Abrir los archivos ODS generados
doc.open_sheet(ods_file1)
doc.open_sheet(ods_file2)
doc.open_sheet(ods_file3)
doc.open_sheet(ods_file4)
doc.open_sheet(ods_file6)
doc.open_sheet(ods_file7)

# Ejemplo de generación de gráfico (debes adaptar según tus necesidades)
# Se inserta un gráfico de barras en la primera hoja con los datos de df1
chart_data_range = 'Sheet1.A1:D10'  # Rango de datos para el gráfico
chart = doc.create_chart('chart', chart_data_range, pyoo.CHART_TYPE_BAR)
chart.set_title('Gráfico de Ejemplo - Consulta 1')
chart.set_x_axis_title('Eje X')
chart.set_y_axis_title('Eje Y')
doc.insert_chart(chart)

# Guardar el documento con los gráficos insertados
doc.save('resultados_consultas.ods')

# Cerrar la conexión con la base de datos PostgreSQL
conn.close()