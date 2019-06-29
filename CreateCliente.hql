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