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