import os
import polars as pl

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

print(ingresos)