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
 COLLECTION ITEMS TERMINATED BY ','
 MAP KEYS TERMINATED BY ':'
 LOCATION '/antonio/empresa/tienda'
 tblproperties("skip.header.line.count" = "1");