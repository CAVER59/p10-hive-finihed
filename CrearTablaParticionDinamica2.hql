use antonio;
CREATE TABLE antonio.tabla_particion_dinamica2
(
    TipoTransaccion String,
    Monto String
)
PARTITIONED BY (TipoCliente string)
STORED AS PARQUET
LOCATION '/antonio/empresa/tabla_particion_dinamica2';