--1 horario con más citas durante el día por peluquería, identificando la comuna -----------------------------------------------------
SELECT DISTINCT ON (id_sede)
    id_sede,
    hora_inicio as Horario_pick,
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
--2 lista de clientes que gastan más dinero por peluquería, indicando comuna del cliente y de peluquería, además de cuanto gasto -----------------------------------------------------
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
--3 lista de peluqueros que ha ganado más por mes durante el 2023, esto por peluquería -----------------------------------------------------
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

--4 lista de clientes hombres que se cortan el pelo y la barba -----------------------------------------------------
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

--6 identificar el horario más concurrido por peluquería durante el 2019 y 2020, desagregados por mes -----------------------------------------------------
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


--7 identificar al cliente que ha tenido las citas más largas por peluquería, por mes-----------------------------------------------------
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

--9 identificar al peluquero que ha trabajado más por mes durante el 2019-----------------------------------------------------
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

--10 identificar lista clientes de totales por comuna, cantidad de peluquerías, cantidad de clientes residentes en la comuna-----------------------------------------------------
WITH PeluqueriasPorComuna AS (
    SELECT 
        comuna_pelu,
        COUNT(DISTINCT id_sede) AS cantidad_peluquerias
    FROM 
        factcitas
    GROUP BY 
        comuna_pelu
),
ClientesPorComuna AS (
    SELECT 
        comuna_cliente,
        COUNT(DISTINCT id_cliente) AS cantidad_clientes_residentes
    FROM 
        dimclientes
    GROUP BY 
        comuna_cliente
)
SELECT 
    COALESCE(pc.comuna_pelu, cc.comuna_cliente) AS comuna,
    COALESCE(pc.cantidad_peluquerias, 0) AS cantidad_peluquerias,
    COALESCE(cc.cantidad_clientes_residentes, 0) AS cantidad_clientes_residentes
FROM 
    PeluqueriasPorComuna pc
FULL OUTER JOIN 
    ClientesPorComuna cc ON pc.comuna_pelu = cc.comuna_cliente
ORDER BY 
    comuna;


