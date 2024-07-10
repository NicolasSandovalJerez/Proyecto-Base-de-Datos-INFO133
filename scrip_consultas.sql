--1-----------------------------------------------------
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
    num_citas DESC;
--2-----------------------------------------------------
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
--3-----------------------------------------------------
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

--4-----------------------------------------------------
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

--6-----------------------------------------------------
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


--7-----------------------------------------------------
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

--8-----------------------------------------------------

--9-----------------------------------------------------
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

--10-----------------------------------------------------



