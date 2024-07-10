
CREATE TABLE public.dimclientes (
	id_cliente int4 NOT NULL,
	nombre_cliente varchar NOT NULL,
	apellido_cliente varchar NOT NULL,
	direccion_cliente varchar NOT NULL,
	comuna_cliente varchar NOT NULL,
	region_cliente varchar NOT NULL,
	sexo bpchar(1) NOT NULL,
	fecha_nacimiento date NOT NULL,
	rut_cliente varchar NOT NULL,
	CONSTRAINT dimclientes_pkey PRIMARY KEY (id_cliente)
);


-- public.dimempleados definition

-- Drop table

-- DROP TABLE public.dimempleados;

CREATE TABLE public.dimempleados (
	id_emple int4 NOT NULL,
	nombre_emple varchar NOT NULL,
	apellido_emple varchar NOT NULL,
	direccion_emplea varchar NOT NULL,
	comuna_emple varchar NOT NULL,
	region_emplea varchar NOT NULL,
	rut_emplea varchar NOT NULL,
	cargo varchar NOT NULL,
	sueldo money NOT NULL,
	id_sede int4 NOT NULL,
	CONSTRAINT dimempleados_pkey PRIMARY KEY (id_emple)
);


-- public.dimservicios definition

-- Drop table

-- DROP TABLE public.dimservicios;

CREATE TABLE public.dimservicios (
	id_serv int4 NOT NULL,
	tipo_serv varchar NOT NULL,
	precio_serv money NOT NULL,
	CONSTRAINT dimservicios_pkey PRIMARY KEY (id_serv)
);


-- public.factcitas definition

-- Drop table

-- DROP TABLE public.factcitas;

CREATE TABLE public.factcitas (
	id_cita int4 NOT NULL,
	id_sede int4 NOT NULL,
	id_emple int4 NOT NULL,
	id_cliente int4 NOT NULL,
	hora_inicio time NOT NULL,
	hora_fin time NOT NULL,
	fecha_cita date NOT NULL,
	total money NOT NULL,
	comuna_pelu varchar NOT NULL,
	comuna_cliente varchar NOT NULL,
	CONSTRAINT factcitas_pkey PRIMARY KEY (id_cita)
);

