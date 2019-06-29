set map.reduce.tasks = 2;
set hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE antonio.cliente_buckets SELECT * FROM antonio.tienda1;
