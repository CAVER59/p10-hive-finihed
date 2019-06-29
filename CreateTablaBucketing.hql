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
