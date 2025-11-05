import os
import polars as pl

ruta = r"\\192.168.3.70\Costos\COSTOS 2025\BALANCE DE COMPROBACION\Informe Ingresos"
nombre_archivo = "Ingresos_clasificados.xlsx"

archivo = os.path.join(ruta, nombre_archivo)
data = pl.read_excel(archivo, sheet_name="Ingresos", schema_overrides={"1" : pl.Int64, "Total general" : pl.Int64})

capita = data.filter(
    pl.col("CuentasMov") == "4110102702"
)


distribucion_porcentual = pl.DataFrame({
    "CuentasMov":['4110102102','4110102602','4110102702','4110102802','4110103302','4110103402','4110104202','4110104402','4110105402','4125100216','4125100310','4130100102','4130100202','4130100602'],
    "Porcentaje":[0.00858850720267853,0.0129675414397561,0.546802357534043,0.0148633152315544,0.00453751774427452,0.0376729843659472,0.00518037293294954,0.0083753276786851,0.308454237545001,0.0079969713590606,0.0302921099976902,0.00553462752014605,0.00329110823991143,0.00544302120830266]
})
 
capita = capita.with_columns(
    pl.struct(pl.all()).rank(method="ordinal").alias("Rango")
)

capita = capita.join(distribucion_porcentual, how="cross")
capita = capita.with_columns(
    (pl.col("Total general")*pl.col("Porcentaje")).alias("Valor Personalizado").cast(pl.Int64, strict=True)
).select(
    "d_mes",
    "CuentasMov_right",
    "1",
    "Valor Personalizado"
)

cambiar_nombres = {
    "d_mes" : "d_mes",
    "CuentasMov_right" : "CuentasMov",
    "1" : "1",
    "Valor Personalizado" : "Total general"
}

capita = capita.rename(cambiar_nombres)

print(capita)

data = data.filter(
    pl.col("CuentasMov") != "4110102702"
)

data = pl.concat([data, capita])

print(data)




clasificacion = pl.read_excel(archivo, sheet_name="Clasificaciones")
#, schema_overrides={"ORDEN" : pl.Int64,})

#['d_mes', 'CuentasMov', '1', 'Total general']
#['Cuenta', 'descripcion Cuenta', 'unidad funcional', 'centro de costo']

data = data.join(clasificacion, left_on="CuentasMov", right_on="Cuenta", how="left", coalesce=True)

nulos_unidades = data.filter(pl.col("unidad funcional").is_null())

nulos_unidades = nulos_unidades.select(["CuentasMov", "descripcion Cuenta", "centro de costo"]).unique()

data = data.filter(pl.col("unidad funcional").is_not_null())

ruta = r"\\192.168.3.70\Costos\COSTOS 2025\ACUMULADOS 2025\ACUMULADOS INFORMES 2025"

nombre_archivo =  "ACUMULADO GENERAL.xlsx"
archivo2 = os.path.join(ruta, nombre_archivo)

mes = pl.read_excel(archivo2, sheet_name="MESES", schema_overrides={"FECHA" : pl.Date,})


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

#print(ingresos)