import random

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

# Listas de nombres y direcciones de peluquerías
nombres_pelu = [
    "Estilo Único", "Cortes Mágicos", "Belleza Total", "Glamour Hair", "Tijeras de Oro",
    "Pelo Perfecto", "Ondas y Más", "Colores Vivos", "Esencia Capilar", "Mechones Divinos",
    "Brillo Natural", "Cabello de Ensueño", "Corte Elegante", "Rizos Perfectos", "Estilo Urbano"
]

direcciones = [
    "Avenida Principal 123", "Calle del Comercio 456", "Paseo de la Moda 789", "Boulevard Estilista 101",
    "Plaza de la Belleza 202", "Callejón del Peinado 303", "Avenida Glamour 404", "Calle Estilo 505",
    "Pasaje del Espejo 606", "Camino de las Tijeras 707", "Avenida del Color 808", "Calle del Secador 909",
    "Plaza del Cepillo 111", "Paseo del Rizo 222", "Avenida de la Creatividad 333"
]

# Lista para almacenar las sentencias SQL
sql_statements = []

# Generar 50 registros
for id_sede in range(1, 51):  
    nombre_pelu = random.choice(nombres_pelu)
    direccion_pelu = random.choice(direcciones)
    region_pelu = random.choice(list(regiones_comunas.keys()))
    comuna_pelu = random.choice(regiones_comunas[region_pelu])

    # Construir la sentencia SQL para insertar el registro
    sql_statement = f"INSERT INTO sede_pelu (id_sede, nombre_pelu, direccion_pelu, comuna_pelu, region_pelu) VALUES ({id_sede}, '{nombre_pelu}', '{direccion_pelu}', '{comuna_pelu}', '{region_pelu}');"
    sql_statements.append(sql_statement)

# Escribir el script SQL en un archivo
with open("insert_sede_pelu.sql", "w") as file:
    file.write("\n".join(sql_statements))

print("El script SQL se ha generado correctamente en el archivo 'insert_sede_pelu.sql'.")