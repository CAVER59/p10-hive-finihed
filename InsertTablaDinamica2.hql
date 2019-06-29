set hive.exec.dynamic.partition.mode=nonstrict;

insert into table antonio.tabla_particion_dinamica2 partition (TipoCliente)
SELECT
  case
    when TipoTransaccion = 'BOL' then 'Boleta'    
    else 'Factura'
  end as TipoTransaccion,
  cast(Monto as decimal(19, 2)) as Monto,
  case
    when Monto < 50 then 'Standard'
    when Monto between 51 and 100 then 'Medium'
    else 'Top'
  end as TipoCliente
FROM antonio.transaccion2;