import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import time
from datetime import datetime, timedelta

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname='db_analisis',
    user='nico',
    password='1234',
    host='localhost',
    port='5432'
)

def execute_query(query):
    return pd.read_sql_query(query, conn)

def plot_line_and_save(df, x, y, title, xlabel, ylabel, filename):
    plt.figure(figsize=(10, 6))
    plt.plot(df[x], df[y], marker='o')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_bar_and_save(df, x, y, title, xlabel, ylabel, filename, xticks_labels=None):
    plt.figure(figsize=(10, 6))
    plt.bar(df[x], df[y])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if xticks_labels:
        plt.xticks(ticks=range(1, 13), labels=xticks_labels, rotation=45)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_multiple_lines_and_save(df, x, y, category, title, xlabel, ylabel, filename):
    categories = df[category].unique()
    plt.figure(figsize=(10, 6))
    for cat in categories:
        subset = df[df[category] == cat]
        plt.plot(subset[x], subset[y], marker='o', label=f'{category}: {cat}')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def time_to_minutes(t):
    return t.hour * 60 + t.minute

# Consulta 1: Horario con más citas durante el día por peluquería, identificando la comuna
query1 = """
SELECT 
    comuna_pelu,
    hora_inicio,
    COUNT(id_cita) AS num_citas
FROM 
    FactCitas
GROUP BY 
    comuna_pelu, 
    hora_inicio
ORDER BY 
    comuna_pelu, 
    hora_inicio;
"""
df1 = execute_query(query1)
df1['hora_inicio'] = pd.to_datetime(df1['hora_inicio'], format='%H:%M:%S').dt.time
df1['hora_inicio_min'] = df1['hora_inicio'].apply(time_to_minutes)
plot_line_and_save(df1, 'hora_inicio_min', 'num_citas', 'Número de Citas por Hora y Comuna', 'Hora Inicio (en minutos)', 'Número de Citas', 'consulta1.png')

# Consulta 2: Lista de clientes que gastan más dinero por peluquería
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

# Consulta 3: Peluqueros que han ganado más por mes durante el 2023
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

# Consulta 4: Clientes hombres que se cortan el pelo y la barba
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
    DimServicios S ON F.id_sede = S.id_serv -- Asumiendo que se relaciona por id_sede (cámbialo si es necesario)
WHERE 
    C.sexo = 'M' 
    AND S.id_serv IN (1, 3);
"""
df4 = execute_query(query4)

# Consulta 6: Identificar el horario más concurrido por peluquería durante el 2019 y 2020
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
df6['hora_inicio'] = pd.to_datetime(df6['hora_inicio'], format='%H:%M:%S').dt.time
df6['hora_inicio_min'] = df6['hora_inicio'].apply(time_to_minutes)
plot_multiple_lines_and_save(df6, 'hora_inicio_min', 'num_citas', 'id_sede', 'Horario Más Concurrido por Peluquería durante 2019 y 2020', 'Hora Inicio (en minutos)', 'Número de Citas', 'consulta6.png')

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

# Consulta 9: Identificar al peluquero que ha trabajado más por mes durante el 2019
query9 = """
WITH RankedCitas AS (
    SELECT 
        id_emple,
        EXTRACT(MONTH FROM fecha_cita) AS mes,
        COUNT(*) AS num_citas,
        ROW_NUMBER() OVER(PARTITION BY EXTRACT(MONTH FROM fecha_cita) ORDER BY COUNT(*) DESC) AS rank
    FROM 
        FactCitas
    WHERE 
        EXTRACT(YEAR FROM fecha_cita) = 2019
    GROUP BY 
        id_emple, 
        EXTRACT(MONTH FROM fecha_cita)
)
SELECT 
    id_emple,
    mes,
    num_citas
FROM 
    RankedCitas
WHERE 
    rank = 1
ORDER BY 
    mes;
"""
df9 = execute_query(query9)
plot_bar_and_save(df9, 'mes', 'num_citas', 'Peluquero que ha Trabajado Más por Mes durante 2019', 'Mes', 'Número de Citas', 'consulta9.png', xticks_labels=['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'])

# Cerrar la conexión
conn.close()
print("Datos cargados y gráficos generados")

