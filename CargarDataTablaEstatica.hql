insert into antonio.tabla_particion_estatica partition(mensual ='201809')
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