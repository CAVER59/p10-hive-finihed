//Lanzar Código archivos hql Jupyter
beeline -u jdbc:hive2://localhost:10000/ -e "CREATE DATABASE antonio;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.cliente1;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.tienda1;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.tabla_particion_estatica;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.transaccion2;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.tabla_particion_estatica;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.cliente_buckets;"
beeline -u jdbc:hive2://localhost:10000/ -e "select * from antonio.tabla_particion_dinamica;"

beeline -u jdbc:hive2://localhost:10000/ -e "show DATABASE antonio;"

//Lanzar archivos que contienen scripts de hive

//Crear la tabla cliente en la base de datos antonio
//Cargue el archivo .hql al servidor
//Ejecuto desde la terminal
beeline -u jdbc:hive2://localhost:10000/ -f "antonio/CreateCliente.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CreateTienda.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CreateTablaEstatica.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CreateTransaccion.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CargarDataTablaEstatica.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CargarDataTablaEstaticaOtraParticion.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CreateTablaBucketing.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/InsertBuckettingTable.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/CrearTablaParticionDinamica.hql"
beeline -u jdbc:hive2://localhost:10000/antonio -f "antonio/InsertTablaDinamica.hql"

1. Tabla simple - OK
Creando una tabla External (autogestionada)

1.1 Ejemplo 1
use antonio;
CREATE EXTERNAL TABLE IF NOT EXISTS antonio.cliente1(
idcliente string COMMENT 'IdCliente',
dni string COMMENT 'DNI',
apellidopaterno string COMMENT 'ApellidoPaterno',
apellidomaterno string COMMENT 'ApellidoMaterno',
nombres string COMMENT 'Nombres',
genero string COMMENT 'Genero',
direccion string COMMENT 'Direccion',
distrito string COMMENT 'Distrito',
correo string COMMENT 'Correo',
telefono1 int COMMENT 'Telefono1',
telefono2 int COMMENT 'Telefono2'
)
COMMENT 'Tabla cliente'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/antonio/empresa/cliente'
tblproperties("skip.header.line.count" = "1");

1.2 Ejemplo 2
use antonio;
CREATE EXTERNAL TABLE IF NOT EXISTS antonio.transaccion2(
idcliente string,
Monto float,
FormatoTransaccion string,
TipoTransaccion string,
IdTienda string,
Fecha date,
IdProducto string,
Unidades integer 
)
COMMENT 'Tabla transacciones'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/antonio/empresa/transaccion'
tblproperties("skip.header.line.count" = "1");


2. Crear tabla estructura compleja - OK

2.1 Ejemplo 1
use antonio;
CREATE EXTERNAL TABLE IF NOT EXISTS antonio.tienda1(
 IdTienda string,
 Sucursal string,
 Distrito string,
 Tipo string,
 DistritoTipo struct<Distrito:STRING,Tipo:string>
 )
 COMMENT 'Tabla tienda'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 LINES TERMINATED BY '\n'
 COLLECTION ITEMS TERMINATED BY ','
 LOCATION '/antonio/empresa/tienda'
 tblproperties("skip.header.line.count" = "1");

2.2 Ejemplo 2
USE trafico;
CREATE EXTERNAL TABLE IF NOT EXISTS trafico.EMP_TRANSPORTEDETALLE (
nombre STRING,
funcionario_id INT, 
codigo_empresa int,
nombre_empresa string,
lugarconst_anios STRUCT<lugarconst:STRING,anios :INT>,
origen_destino MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
LOCATION '/datalake/landing/transporte_detalle';

3. Crear tabla con participación estática OK
use ctic;
CREATE TABLE antonio.tabla_particion_estatica
(
    Fecha String,
    TipoCliente String,
    TipoTransaccion String,
    Monto String
)
PARTITIONED BY (mensual STRING)
STORED AS PARQUET
LOCATION '/antonio/empresa/tabla_particion_estatica';

//Agregamos data
insert into antonio.tabla_particion_estatica partition(mensual ='201909')
select 
  from_unixtime (unix_timestamp(Concat(substring(fecha,1,4),SUBSTRING(Fecha,5,2), SUBSTRING(Fecha,7,2)), 'yyyyMMdd'), 'yyyy-MM-dd') as Fecha,
  case
    when Monto < 50 then 'Standard'
    when Monto between 51 and 100 then 'Medium'
    else 'Top'
  end as TipoCliente,
  case
    when TipoTransaccion = 'BOL' then 'Boleta'    
    else 'Factura'
  end as TipoTransaccion,
  cast(Monto as decimal(19, 2)) as Monto
from antonio.transaccion2;

4. Crear tabla con participación bucketing OK
LIMA CHICLAYO TACNA
X = Y (Se trata de tener el mismo peso)

BUCKET 1  X MB
LIMA 
CHICLAYO 

BUCKET 2 Y MB
CHICLAYO
TACNA


use antonio;
CREATE EXTERNAL TABLE IF NOT EXISTS antonio.cliente_buckets (
 IdTienda string,
 Sucursal string,
 Distrito string,
 Tipo string,
 DistritoTipo struct<Distrito:STRING,Tipo:string>
 )
 CLUSTERED BY (Sucursal) INTO 2 BUCKETS 
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '|'
 COLLECTION ITEMS TERMINATED BY ','
 MAP KEYS TERMINATED BY ':'
LOCATION '/antonio/empresa/tabla_bucketing';

--Engines de Hive: Map Reduce, Spark y Tez
set map.reduce.tasks = 2;
set hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE antonio.cliente_buckets SELECT * FROM antonio.tienda1;

5. Crear tabla con partición dinamica OK
use antonio;
CREATE TABLE antonio.tabla_particion_dinamica
(
    TipoCliente String,
    TipoTransaccion String,
    Monto String
)
PARTITIONED BY (Fecha string)
STORED AS PARQUET
LOCATION '/antonio/empresa/tabla_particion_dinamica';


set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ctic.tabla_particion_dinamica partition (Fecha)
SELECT
  case
    when Monto < 50 then 'Standard'
    when Monto between 51 and 100 then 'Medium'
    else 'Top'
  end as TipoCliente,
  case
    when TipoTransaccion = 'BOL' then 'Boleta'    
    else 'Factura'
  end as TipoTransaccion,
  cast(Monto as decimal(19, 2)) as Monto,
  from_unixtime (unix_timestamp(Concat(substring(fecha,1,4),SUBSTRING(Fecha,5,2), SUBSTRING(Fecha,7,2)), 'yyyyMMdd'), 'yyyy-MM-dd') as Fecha
FROM ctic.transaccion2;

5. Transformaciones con UDFs (User Defined Functions) nativas OK
Ej: concat, substring, cast, avg, sum, etc.
(Más ejemplos en el enlace: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF)
5.1 Ejemplo 1
SELECT
  case
    when Monto < 50 then 'Standard'
    when Monto between 51 and 100 then 'Medium'
    else 'Top'
  end as TipoCliente,
  case
    when TipoTransaccion = 'BOL' then 'Boleta'    
    else 'Factura'
  end as TipoTransaccion,
  cast(Monto as decimal(19, 2)) as Monto,
  from_unixtime (unix_timestamp(Concat(substring(fecha,1,4),SUBSTRING(Fecha,5,2), SUBSTRING(Fecha,7,2)), 'yyyyMMdd'), 'yyyy-MM-dd') as Fecha
FROM ctic.transaccion2;

5.2 Ejemplo 2
SELECT
  from_unixtime(unix_timestamp(CONCAT(SUBSTRING(Fecha, 7, 4), SUBSTRING(Fecha, 4, 2), SUBSTRING(Fecha, 1, 2)), 'yyyyMMdd'), 'yyyy-MM-dd') AS FecRegistro,
  UPPER(profundida) AS profundida,
  CASE
    WHEN profundida < 10 THEN 'BAJA'
    WHEN profundida BETWEEN 10 AND 50 THEN 'MEDIA'
    ELSE 'ALTA'
  END AS medidaprod,
  geom,
  magnitud__
FROM sesion_dos.tabla_externa;

5.3 Ejemplo 3
SELECT
  from_unixtime(unix_timestamp(CONCAT(SUBSTRING(Fecha, 7, 4), SUBSTRING(Fecha, 4, 2), SUBSTRING(Fecha, 1, 2)), 'yyyyMMdd'), 'yyyy-MM-dd') AS FecRegistro,
  COUNT(distinct geom)
FROM sesion_dos.tabla_externa
GROUP BY from_unixtime(unix_timestamp(CONCAT(SUBSTRING(Fecha, 7, 4), SUBSTRING(Fecha, 4, 2), SUBSTRING(Fecha, 1, 2)), 'yyyyMMdd'), 'yyyy-MM-dd');

6. Joins
SELECT
DISTINCT
  a.fid,
  b.geom,
  b.fecha,
  b.magnitud__,
  b.profundida
FROM sesion_dos.tabla_externa a
INNER JOIN sesion_dos.tabla_gestionada b
  ON a.fid = b.fid;