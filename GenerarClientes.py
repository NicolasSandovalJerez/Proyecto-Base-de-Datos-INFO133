import random
import string
from datetime import date, timedelta

# Función para generar un RUT válido
def generar_rut():
    rut = ''.join(random.choices(string.digits, k=8))
    dv = sum(map(lambda x, y: (x * int(y)) % 11, [2, 3, 4, 5, 6, 7] * int(len(rut) / 6 + 1), rut[::-1])) % 11
    dv = str(11 - dv) if dv >= 1 else '0'
    return f"{rut}-{dv}"

# Función para generar una fecha de nacimiento aleatoria
def generar_fecha_nacimiento():
    start_date = date(1950, 1, 1)
    end_date = date(2005, 12, 31)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

# Diccionario de regiones y sus comunas correspondientes
regiones_comunas = {
    "Metropolitana": ["Santiago", "Providencia", "Las Condes", "Maipu"],
    "Valparaiso": ["Valparaiso", "Quilpue", "Villa Alemana", "Concon"],
    "Biobio": ["Concepcion", "Talcahuano", "Los Angeles", "Chillan", "Coronel"],
    "Antofagasta": ["Antofagasta", "Calama", "Tocopilla", "Mejillones", "Taltal"],
    "Araucania": ["Temuco", "Villarrica", "Angol", "Victoria", "Lautaro"],
    "Los Rios" : ["Valdivia", "Corral", "Rio Bueno", "La Union", "Mafil", "Lanco", "Mariquina"],
    "Los Lagos": ["Osorno", "Puerto Montt", "Castro", "Puerto Varas"]
}

# Listas de nombres, apellidos y direcciones
nombres = ["Juan", "Maria", "Pedro", "Ana", "Carlos", "Luisa", "Jose", "Laura", "Miguel", "Carmen"]
apellidos = ["Gonzalez", "Rodriguez", "Perez", "Sanchez", "Martinez", "Garcia", "Lopez", "Hernandez", "Diaz", "Torres"]
direcciones = ["Calle Falsa 123", "Avenida Siempre Viva 456", "Pasaje Olvidado 789", "Camino Perdido 159", "Boulevard Eterno 753", "Avenida Lira 101", "Avenida Matta 202", "Paseo Bulnes 303", "Calle Londres 404", "Avenida Brasil 505", "Calle Bellavista 606", "Avenida Independencia 707", "Calle San Diego 808", "Avenida Santa Rosa 909", "Calle Merced 111", "Avenida Recoleta 222", "Calle Esmeralda 333", "Avenida OHiggins 444", "Calle Suecia 555", "Avenida Tobalaba 666"]

# Lista para almacenar las sentencias SQL
sql_statements = []

# Generar 700 
for id_cliente in range(1,701):
    nombre = random.choice(nombres)
    apellido = random.choice(apellidos)
    direccion = random.choice(direcciones)
    region = random.choice(list(regiones_comunas.keys()))
    comuna = random.choice(regiones_comunas[region])
    sexo = random.choice(["M", "F"])
    fecha_nacimiento = generar_fecha_nacimiento()
    rut_cliente = generar_rut()
    
    # Construir la sentencia SQL para insertar el registro
    sql_statement = f"INSERT INTO clientes (id_cliente, nombre_cliente, apellido_cliente, direccion_cliente, comuna_cliente, region_cliente, sexo, fecha_nacimiento, rut_cliente) VALUES ('{id_cliente}', '{nombre}', '{apellido}', '{direccion}', '{comuna}', '{region}', '{sexo}', '{fecha_nacimiento}', '{rut_cliente}');"
    sql_statements.append(sql_statement)

# Escribir el script SQL en un archivo
with open("insert_clientes.sql", "w") as file:
    file.write("\n".join(sql_statements))

print("El script SQL se ha generado correctamente en el archivo 'insert_clientes.sql'.")
