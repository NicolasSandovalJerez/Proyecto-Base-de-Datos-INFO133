import random
num_citas = 75000  

# Asumimos el número de servicios y productos
num_servicios = 20  
num_productos = 50  

# Lista para almacenar las sentencias SQL
sql_statements = []

# Para cada cita, generamos entre 1 y 3 detalles
for id_cita in range(1, num_citas + 1):
    num_detalles = random.randint(1, 3)
    
    # Creamos un conjunto para asegurar que no haya duplicados de (id_serv, id_prod) para la misma cita
    detalles_unicos = set()
    
    for _ in range(num_detalles):
        while True:
            id_serv = random.randint(1, num_servicios)
            id_prod = random.randint(1, num_productos)
            if (id_serv, id_prod) not in detalles_unicos:
                detalles_unicos.add((id_serv, id_prod))
                break
        
        cantidad = random.randint(1, 5)  #cantidad puede ser entre 1 y 5
        
        # Construir la sentencia SQL para insertar el registro
        sql_statement = f"""INSERT INTO cita_detalle (id_cita, id_serv, id_prod, cantidad) VALUES ({id_cita}, {id_serv}, {id_prod}, {cantidad});"""
        sql_statements.append(sql_statement)

# Escribir el script SQL en un archivo
with open("insert_cita_detalle.sql", "w") as file:
    file.write("\n".join(sql_statements))

print("El script SQL se ha generado correctamente en el archivo 'insert_cita_detalle.sql'.")