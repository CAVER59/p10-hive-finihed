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