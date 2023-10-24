# Importa las bibliotecas necesarias
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Crea una sesión de Spark
spark = SparkSession.builder.appName("AnalisisDeVentas").getOrCreate()

# Paso 1: Cargar el archivo de texto como un RDD
sc = SparkContext.getOrCreate()
rdd = sc.textFile("/content/notas.txt")

# Paso 2: Convierte cada línea en una tupla y maneja errores
def parse_line(line):
    try:
        parts = line.split(",")
        if len(parts) == 3:
            return Row(producto=parts[0], precio=float(parts[1]), cantidad=int(parts[2]))
        else:
            return Row(producto="Error", precio=0.0, cantidad=0)
    except:
        return Row(producto="Error", precio=0.0, cantidad=0)

rdd = rdd.map(parse_line)

# Filtra las líneas con errores
rdd = rdd.filter(lambda x: x.producto != "Error")

# Convierte el RDD en un DataFrame
df = spark.createDataFrame(rdd)

# Paso 3: Calcular el total de ventas por producto utilizando reduceByKey
ventas_por_producto = df.rdd.map(lambda x: (x.producto, x.precio * x.cantidad)).reduceByKey(lambda x, y: x + y)

# Paso 4: Calcular el total de ventas para toda la tienda utilizando reduce
total_ventas_tienda = ventas_por_producto.map(lambda x: x[1]).reduce(lambda x, y: x + y)

# Paso 5: Encontrar el producto más vendido utilizando sortBy
producto_mas_vendido = ventas_por_producto.sortBy(lambda x: x[1], ascending=False).first()

# Imprime los resultados
print("Total de ventas por producto:")
for producto, ventas in ventas_por_producto.collect():
    print(f"{producto}: {ventas}")

print(f"Total de ventas para toda la tienda: {total_ventas_tienda}")
print(f"Producto más vendido: {producto_mas_vendido[0]} con {producto_mas_vendido[1]} ventas")

# Detén la sesión de Spark
spark.stop()
