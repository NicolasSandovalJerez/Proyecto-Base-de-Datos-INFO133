import random
import string

# Diccionario de regiones y sus comunas correspondientes
regiones_comunas = {
    "Metropolitana": ["Santiago", "Providencia", "Las Condes", "Maipu"],
    "Valparaiso": ["Valparaiso", "Quilpue", "Villa Alemana", "Concon"],
    "Biobio": ["Concepcion", "Talcahuano", "Los Angeles", "Chillan", "Coronel"],
    "Antofagasta": ["Antofagasta", "Calama", "Tocopilla", "Mejillones", "Taltal"],
    "Araucania": ["Temuco", "Villarrica", "Angol", "Victoria", "Lautaro"],
    "Los Rios": ["Valdivia", "Corral", "Rio Bueno", "La Union", "Mafil", "Lanco", "Mariquina"],
    "Los Lagos": ["Osorno", "Puerto Montt", "Castro", "Puerto Varas"]
}

# Listas de nombres, apellidos, direcciones y cargos
nombres = ["Juan", "Maria", "Pedro", "Ana", "Carlos", "Luisa", "Jose", "Laura", "Miguel", "Carmen"]
apellidos = ["Gonzalez", "Rodriguez", "Perez", "Sanchez", "Martinez", "Garcia", "Lopez", "Hernandez", "Diaz", "Torres"]
direcciones = ["Calle Falsa 123", "Avenida Siempre Viva 456", "Pasaje Olvidado 789", "Camino Perdido 159", "Boulevard Eterno 753"]
cargos = ["Peluquero/a", "Estilista", "Recepcionista", "Colorista", "Manicurista", "Pedicurista", "Barbero", "Maquillador/a", "Asistente"]

# Función para generar un RUT válido
def generar_rut():
    rut = ''.join(random.choices(string.digits, k=8))
    dv = sum(map(lambda x, y: (x * int(y)) % 11, [2, 3, 4, 5, 6, 7] * int(len(rut) / 6 + 1), rut[::-1])) % 11
    dv = str(11 - dv) if dv > 1 else '0' if dv == 0 else 'K'
    return f"{rut}-{dv}"

# Lista para almacenar las sentencias SQL
sql_statements = []

# Generar 100 registros 
for id_emple in range(1, 201): 
    nombre_emple = random.choice(nombres)
    apellido_emple = random.choice(apellidos)
    direccion_emplea = random.choice(direcciones)
    region_emplea = random.choice(list(regiones_comunas.keys()))
    comuna_emple = random.choice(regiones_comunas[region_emplea])
    rut_emplea = generar_rut()
    cargo = random.choice(cargos)
    sueldo = random.randint(400000, 1500000)
    id_sede = random.randint(1, 50)  

    # Construir la sentencia SQL para insertar el registro
    sql_statement = f"""INSERT INTO empleados (id_emple, nombre_emple, apellido_emple, direccion_emplea, comuna_emple, region_emplea, rut_emplea, cargo, sueldo, id_sede) VALUES ({id_emple}, '{nombre_emple}', '{apellido_emple}', '{direccion_emplea}', '{comuna_emple}', '{region_emplea}', '{rut_emplea}', '{cargo}', '{sueldo}', '{id_sede}');"""
    sql_statements.append(sql_statement)

# Escribir el script SQL en un archivo
with open("insert_empleados.sql", "w") as file:
    file.write("\n".join(sql_statements))

print("El script SQL se ha generado correctamente en el archivo 'insert_empleados.sql'.")