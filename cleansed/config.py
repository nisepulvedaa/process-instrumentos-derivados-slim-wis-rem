proyecto="deinsoluciones-serverless"
nombre_proceso="instrumentos-derivados-slim-wis-rem" #nombre-proceso
periodicidad="diario"# diario|mensual|esporadico
##CLEANSED
dataset_cleansed_zone="dev_cleansed_zone"
nombre_tabla_cleansed="sat_instrumento_derivado_dia_wis_rem"
campo_fecha_tabla_cleansed="ins_der_fec_periodo"
fecha_ejecucion="2025-04-24"
###CREATE
ddl_satelite="CREATE TABLE IF NOT EXISTS `deinsoluciones-serverless.dev_cleansed_zone.sat_instrumento_derivado_dia_wis_rem` (hk_cod_operacion BYTES,ins_der_fec_periodo DATE,ins_der_cod_filial STRING,ins_der_rut_deudor INT64,ins_der_tipo_derivado STRING,ins_der_monto NUMERIC,ins_der_ind_netting STRING,sistema_origen STRING, fecha_carga TIMESTAMP) PARTITION BY ins_der_fec_periodo;"
ddl_hub="CREATE TABLE IF NOT EXISTS `deinsoluciones-serverless.dev_cleansed_zone.hub_operacion_derivado_wis_rem` (hk_cod_operacion BYTES,cod_operacion STRING,sistema_origen STRING, fecha_carga TIMESTAMP);"
ddl_link=""
## INSERT
insert_satelite="INSERT INTO `deinsoluciones-serverless.dev_cleansed_zone.sat_instrumento_derivado_dia_wis_rem` SELECT DISTINCT SAFE_CAST(TO_HEX(MD5(UPPER(CONCAT(cod_operacion)))) AS BYTES) AS hk_cod_operacion,fec_periodo AS ins_der_fec_periodo,cod_filial AS ins_der_cod_filial,CAST(rut_deudor AS INT64) AS ins_der_rut_deudor,tip_derivado AS ins_der_tipo_derivado,CAST(monto AS NUMERIC) AS ins_der_monto,ind_netting AS ins_der_ind_netting,sistema_origen,CAST(CURRENT_DATETIME('America/Santiago') AS TIMESTAMP) AS fecha_carga FROM deinsoluciones-serverless.dev_raw_zone.tbl_instrumento_derivado_dia_wis_rem WHERE fec_periodo = '2025-04-24';"
insert_hub="INSERT INTO `deinsoluciones-serverless.dev_cleansed_zone.hub_operacion_derivado_wis_rem` SELECT SAFE_CAST(TO_HEX(MD5(UPPER(CONCAT(raw.cod_operacion)))) AS BYTES) AS hk_cod_operacion,raw.cod_operacion,raw.sistema_origen,CAST(CURRENT_DATETIME('America/Santiago') AS TIMESTAMP) AS fecha_carga FROM `deinsoluciones-serverless.dev_raw_zone.tbl_instrumento_derivado_dia_wis_rem` raw LEFT JOIN `deinsoluciones-serverless.dev_cleansed_zone.hub_operacion_derivado_wis_rem` hub ON SAFE_CAST(TO_HEX(MD5(UPPER(CONCAT(raw.cod_operacion)))) AS BYTES) = hub.hk_cod_operacion WHERE hub.cod_operacion IS NULL AND raw.fec_periodo = '2025-04-24';"
insert_link=""
##CORREO
correos_destinatarios="ni.sepulvedaa@gmail.com"