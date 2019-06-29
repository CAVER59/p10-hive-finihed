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