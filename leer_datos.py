import os
import polars as pl


ruta = r"\\192.168.3.70\Costos\COSTOS 2025\ACUMULADOS 2025\ACUMULADOS INFORMES 2025"
nombre_archivo =  "ACUMULADO GENERAL.xlsx"
archivo = os.path.join(ruta, nombre_archivo)

data = pl.read_excel(archivo, sheet_name="Hoja1", schema_overrides={"ORDEN" : pl.Int64,})
mes = pl.read_excel(archivo, sheet_name="MESES", schema_overrides={"FECHA" : pl.Date,})
centros_clasificacion = pl.read_excel(archivo, sheet_name="centro_costo")
atributos = pl.read_excel(archivo, sheet_name="atributo")


filtros = ['AREA','TOTAL COSTOS DIRECTOS','TOTAL COSTOS INDIRECTOS','TOTAL PARA DISTRIBUIR COMUNICACIÓN','TOTAL PARA DISTRIBUCIÓN DE GASTOS GENERALES','TOTAL PARA DISTRIBUIR PROCESOS DE APOYO ADMINISTRATIVO','TOTAL PARA DISTRIBUIR PROCESOS DE APOYO ASISTENCIAL','TOTAL PARA DISTRIBUICIN DE ADMINISTRACION','%','TOTAL','%2','TOTAL MAS CUENTAS MEDICAS','%3', '__UNNAMED__51','MANTENIMIENTO', 'DISTRIBUCION ORIENTACION Y VIGILANCIA', 'DISTRIBUCION SERVICIOS GENERALES', 'DISTRIBUCION CITAS MEDICAS ', 'AMBULANCIA', 'CAMILLEROS', 'DISTRIBUCION LAVANDERIA', 'DIRECCION CLINICA', 'PROGRAMA IAMI - AIEPI', 'SALUD MENTAL', 'EPIDEMIOLOGIA', 'SEGURIDAD DEL PACIENTE', 'DISTRIBUCION SERVICIO FARMACEUTICO', 'CARGA ADMINISTRATIVA', 'CARGA ASEGURADORA',"CARGA ADMINISTRATIVA"]

data = data.drop(filtros)

data = data.filter(
    ~pl.col("CENTRO DE COSTOS").is_in(["APOYO TERAPEUTICO", "APOYO DIAGNOSTICO", "MEDICINA ESPECIALIZADA", "MUNICIPIOS", "ODONTOLOGIA ESPECIALIZADA", "PROMOCION Y PREVENCION A", "SERVICIO FARMACÉUTICO A", "TOTAL", "TOTAL GENERAL", ""]) & pl.col("CENTRO DE COSTOS").is_not_null()
)

# data = data.filter(
#     pl.col("CENTRO DE COSTOS").is_in(["Total"])
# )

data_largo = data.unpivot(
    ['TOTAL RELACION LABORAL', 'HONORARIOS', 'CONSUMOS DE INSUMOS', 'MEDICAMENTOS ', 'DISPOSITIVOS MEDICOS FORMULADOS', 'DISPOSITIVOS MEDICOS DE CONSUMO', 'ALIMENTACION HOSPITALARIA', 'OXIGENO-     GAS-      AIRE                                  ', 'SANGRE', 'OTROS COSTOS DIRECTOS', 'ARRENDAMIENTOS', 'DEPRECIACION ACTIVOS FIJOS', 'DEPRECIACION EDIFICIO', 'INCINERACION', 'AGUA', 'ENERGIA', 'COMUNICACIÓN', 'GASTOS GENERALES', 'CUENTAS MEDICAS'], 
    index=['MES', 'ORDEN', 'CENTRO DE COSTOS'],
).filter(pl.col("value") != 0).with_columns(
    pl.col("value").cast(pl.Int64, strict=True),
    pl.col("ORDEN").cast(pl.Int64, strict=True),
)

servicio_farmaceutico = data_largo.filter(
    pl.col("variable").is_in(['MEDICAMENTOS ', 'DISPOSITIVOS MEDICOS FORMULADOS', "MEDICAMENTOS"])
).group_by("MES").agg(pl.col("value").sum().cast(pl.Int64, strict=True)).with_columns(
    pl.lit(9).alias("ORDEN").cast(pl.Int64, strict=True),
    pl.lit("SERVICIO FARMACÉUTICO").alias("CENTRO DE COSTOS").cast(pl.String),
    pl.lit("MEDICAMENTOS").alias("variable").cast(pl.String)
).select(
    "MES",
    "ORDEN",
    "CENTRO DE COSTOS",
    "variable",
    "value",
).with_columns(
    pl.col("value").cast(pl.Int64, strict=True)
)

#servicio_farmaceutico.write_excel("servicio_farmaceutico.xlsx")
#print(servicio_farmaceutico)
#TODO: revisar que no me esta aplicando el filtro
data_largo = data_largo.filter(
    ~(pl.col("variable").is_in(['MEDICAMENTOS ', 'DISPOSITIVOS MEDICOS FORMULADOS']))    
).with_columns(
    pl.col("value").cast(pl.Int64, strict=True),
    pl.col("ORDEN").cast(pl.Int64, strict=True),
)

data_total = pl.concat([data_largo, servicio_farmaceutico]).with_columns(
    pl.col("value").cast(pl.Int64, strict=True),
    pl.col("variable").cast(pl.String),
    pl.col("CENTRO DE COSTOS").cast(pl.String),
    pl.col("ORDEN").cast(pl.Int64, strict=True),
)    

data_total = data_total.join(mes, on="MES", how="left", coalesce=True)
data_total = data_total.join(centros_clasificacion, on="CENTRO DE COSTOS", how="left", coalesce=True)
data_total = data_total.join(atributos, on="variable", how="left", coalesce=True)

data_total = data_total.drop_nulls("Atributo")
data_total = data_total.with_columns(
    (pl.col("value")*-1).alias("Valor Personalizado")
)
#data_total.write_excel("data_total.xlsx")
# print(data_total.head(16))

gasto_y_costo = data_total.select(["FECHA", "MES", "Clasificacion 2", "CENTRO DE COSTOS","variable", "value", "Atributo", "Valor Personalizado"])
#data_total.write_excel("data_total.xlsx")
#\\192.168.3.70\Costos\COSTOS 2025\ACUMULADOS 2025\ACUMULADOS INFORMES 2025

nuevos_nombres = {
    "FECHA" : "FECHA",
    "MES" : "MES",
    "Clasificacion 2" : "UNIDAD FUNCIONAL",
    "CENTRO DE COSTOS" : "CENTRO DE COSTOS",
    "variable" : "ATRIBUTO",
    "value" : "VALOR",
    "Atributo" : "CLASIFICACION",
    "Valor Personalizado" : "VALOR PERSONALIZADO"
}

gasto_y_costo = gasto_y_costo.rename(nuevos_nombres)

ruta = r"\\192.168.3.70\Costos\COSTOS 2025\BALANCE DE COMPROBACION\Informe Ingresos"
nombre_archivo = "Ingresos_clasificados.xlsx"

archivo = os.path.join(ruta, nombre_archivo)
data = pl.read_excel(archivo, sheet_name="Ingresos", schema_overrides={"1" : pl.Int64, "Total general" : pl.Int64})
clasificacion = pl.read_excel(archivo, sheet_name="Clasificaciones")
#, schema_overrides={"ORDEN" : pl.Int64,})

#['d_mes', 'CuentasMov', '1', 'Total general']
#['Cuenta', 'descripcion Cuenta', 'unidad funcional', 'centro de costo']

data = data.join(clasificacion, left_on="CuentasMov", right_on="Cuenta", how="left", coalesce=True)

nulos_unidades = data.filter(pl.col("unidad funcional").is_null())

nulos_unidades = nulos_unidades.select(["CuentasMov", "descripcion Cuenta", "centro de costo"]).unique()

data = data.filter(pl.col("unidad funcional").is_not_null())

data = data.join(mes, on="d_mes", how="left", coalesce=True)

data = data.with_columns(
    pl.lit("Ingresos").alias("Atributo"),
    (pl.col("Total general")*1).alias("Valor Personalizado")
)

ingresos = data.select(["FECHA", "MES", 'unidad funcional', 'centro de costo','descripcion Cuenta', "Total general", "Atributo", "Valor Personalizado"])

#cambio de nombres
nuevos_nombres = {
    "FECHA" : "FECHA",
    "MES" : "MES",
    "unidad funcional" : "UNIDAD FUNCIONAL",
    "centro de costo" : "CENTRO DE COSTOS",
    "descripcion Cuenta" : "ATRIBUTO",
    "Total general" : "VALOR",
    "Atributo" : "CLASIFICACION",
    "Valor Personalizado" : "VALOR PERSONALIZADO"
}

ingresos = ingresos.rename(nuevos_nombres)

completo = pl.concat([gasto_y_costo, ingresos])
completo.write_excel("Informe_Financiero.xlsx")
print("Informe Generado Exitosamente!!!!")
print("~" * 50)
print("Revisar errores en el ingreso")
print(nulos_unidades)
print("~" * 50)