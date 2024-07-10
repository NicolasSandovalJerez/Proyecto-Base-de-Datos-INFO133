import tkinter as tk
from tkinter import ttk, messagebox
import psycopg2

class AdminApp:
    def __init__(self, master):
        self.master = master
        self.master.title("Administrador de Base de Datos")
        self.master.geometry("400x300")

        self.conexion = None
        self.create_widgets()

    def create_widgets(self):
        # Frame principal
        main_frame = ttk.Frame(self.master, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Botón de conexión
        ttk.Button(main_frame, text="Conectar a la BD", command=self.mostrar_dialogo_contrasena).pack(pady=10)

        # Notebook para pestañas
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Pestañas para cada tabla
        self.create_tab(self.notebook, "Clientes", self.insertar_cliente)
        self.create_tab(self.notebook, "Empleados", self.insertar_empleado)
        self.create_tab(self.notebook, "Servicios", self.insertar_servicio)
        self.create_tab(self.notebook, "Productos", self.insertar_producto)
        self.create_tab(self.notebook, "Citas", self.insertar_cita)
        self.create_tab(self.notebook, "Sedes", self.insertar_sede)

    def create_tab(self, notebook, title, command):
        frame = ttk.Frame(notebook, padding="10")
        notebook.add(frame, text=title)
        ttk.Button(frame, text=f"Insertar {title}", command=command).pack()

    def mostrar_dialogo_contrasena(self):
        self.dialogo = tk.Toplevel(self.master)
        self.dialogo.title("Ingresar Contraseña")

        ttk.Label(self.dialogo, text="Contraseña:").pack(padx=10, pady=10)
        self.entry_contrasena = ttk.Entry(self.dialogo, show="*")
        self.entry_contrasena.pack(padx=10, pady=10)

        ttk.Button(self.dialogo, text="Conectar", command=self.conectar_bd).pack(pady=10)

    def conectar_bd(self):
        contrasena = self.entry_contrasena.get()
        self.dialogo.destroy()

        try:
            self.conexion = psycopg2.connect(
                host="localhost",
                database="db_proyecto",
                user="nico",
                password=contrasena
            )
            messagebox.showinfo("Conexión Exitosa", "Conexión establecida con la base de datos.")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error de Conexión", f"Error al conectar a PostgreSQL: {error}")

    def insertar_cliente(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Cliente")

        fields = ['Nombre', 'Apellido', 'Dirección', 'Comuna', 'Región', 'Sexo (M/F)', 'Fecha Nacimiento (aaaa-mm-dd)', 'RUT (12345678-9)']
        examples = ['', '', '', '', '', 'M/F', 'aaaa-mm-dd', '12345678-9']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_cliente(entries)).pack()

    def submit_cliente(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO clientes (nombre_cliente, apellido_cliente, direccion_cliente, 
                comuna_cliente, region_cliente, sexo, fecha_nacimiento, rut_cliente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_cliente;
            """, values)
            
            id_cliente = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Cliente insertado con ID: {id_cliente}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar cliente: {error}")
        finally:
            cursor.close()

    def insertar_empleado(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Empleado")

        fields = ['Nombre', 'Dirección', 'Comuna', 'Región', 'Apellido', 'RUT (12345678-9)', 'Cargo', 'Sueldo', 'ID Sede']
        examples = ['', '', '', '', '', '12345678-9', '', '', '']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_empleado(entries)).pack()

    def submit_empleado(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO empleados (nombre_emple, direccion_emplea, comuna_emple, region_emplea, 
                apellido_emple, rut_emplea, cargo, sueldo, id_sede)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, (SELECT MAX(id_sede) FROM sede_pelu))
                RETURNING id_emple;
            """, values)
            
            id_emple = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Empleado insertado con ID: {id_emple}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar empleado: {error}")
        finally:
            cursor.close()

    def insertar_servicio(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Servicio")

        fields = ['Tipo', 'Precio']
        examples = ['', '']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_servicio(entries)).pack()

    def submit_servicio(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO servicio (tipo_serv, precio_serv)
                VALUES (%s, %s)
                RETURNING id_serv;
            """, values)
            
            id_serv = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Servicio insertado con ID: {id_serv}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar servicio: {error}")
        finally:
            cursor.close()

    def insertar_producto(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Producto")

        fields = ['Nombre', 'Precio']
        examples = ['', '']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_producto(entries)).pack()

    def submit_producto(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO producto (nombre_prod, precio_prod)
                VALUES (%s, %s)
                RETURNING id_prod;
            """, values)
            
            id_prod = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Producto insertado con ID: {id_prod}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar producto: {error}")
        finally:
            cursor.close()

    def insertar_cita(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Cita")

        fields = ['ID Sede', 'ID Empleado', 'ID Cliente', 'Hora de Inicio (hh:mm:ss)', 'Hora de Fin (hh:mm:ss)', 'Fecha de Cita (aaaa-mm-dd)', 'Total']
        examples = ['', '', '', 'hh:mm:ss', 'hh:mm:ss', 'aaaa-mm-dd', '']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_cita(entries)).pack()

    def submit_cita(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO cita (id_sede, id_emple, id_cliente, hora_inicio, hora_fin, fecha_cita, total)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id_cita;
            """, values)
            
            id_cita = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Cita insertada con ID: {id_cita}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar cita: {error}")
        finally:
            cursor.close()

    def insertar_sede(self):
        if not self.conexion:
            messagebox.showerror("Error", "No hay conexión a la base de datos.")
            return

        window = tk.Toplevel(self.master)
        window.title("Insertar Sede")

        fields = ['Nombre', 'Dirección', 'Comuna', 'Región']
        examples = ['', '', '', '']
        entries = []

        for field, example in zip(fields, examples):
            row = ttk.Frame(window)
            row.pack(fill=tk.X, padx=5, pady=5)
            ttk.Label(row, text=field, width=25).pack(side=tk.LEFT)
            entry = ttk.Entry(row)
            entry.insert(0, example)  # Inserta el ejemplo en el Entry
            entry.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
            entries.append(entry)

        ttk.Button(window, text="Insertar", command=lambda: self.submit_sede(entries)).pack()

    def submit_sede(self, entries):
        values = [entry.get() for entry in entries]

        cursor = self.conexion.cursor()
        try:
            cursor.execute("""
                INSERT INTO sede_pelu (nombre_pelu, direccion_pelu, comuna_pelu, region_pelu)
                VALUES (%s, %s, %s, %s)
                RETURNING id_sede;
            """, values)
            
            id_sede = cursor.fetchone()[0]
            self.conexion.commit()
            messagebox.showinfo("Éxito", f"Sede insertada con ID: {id_sede}")
        except (Exception, psycopg2.Error) as error:
            messagebox.showerror("Error", f"Error al insertar sede: {error}")
        finally:
            cursor.close()

def main():
    root = tk.Tk()
    app = AdminApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()