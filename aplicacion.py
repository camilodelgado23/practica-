# Importar las bibliotecas necesarias
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.appName("AnalisisVentas").getOrCreate()
sc = spark.sparkContext

# Paso 1: Cargar el archivo de texto como un RDD
rdd = sc.textFile("ruta/al/archivo.txt")

# Paso 2: Convertir cada línea en una tupla con tres elementos: nombre del producto, precio y cantidad
def parse_line(line):
    parts = line.split(",")
    product = parts[0]
    price = float(parts[1])
    quantity = int(parts[2])
    return (product, price, quantity)

parsed_rdd = rdd.map(parse_line)

# Paso 3: Calcular el total de ventas por producto utilizando reduceByKey
total_ventas_por_producto = parsed_rdd.map(lambda x: (x[0], x[1] * x[2])) \
                                     .reduceByKey(lambda a, b: a + b)

# Paso 4: Calcular el total de ventas para toda la tienda utilizando reduce
total_ventas_tienda = parsed_rdd.map(lambda x: x[1] * x[2]) \
                               .reduce(lambda a, b: a + b)

# Paso 5: Encontrar el producto más vendido utilizando sortBy
producto_mas_vendido = total_ventas_por_producto.sortBy(lambda x: x[1], ascending=False).first()

# Imprimir los resultados
print("Total de ventas por producto:")
for product, total in total_ventas_por_producto.collect():
    print(f"{product}: {total}")

print(f"Total de ventas para toda la tienda: {total_ventas_tienda}")
print(f"Producto más vendido: {producto_mas_vendido[0]} con un total de {producto_mas_vendido[1]} unidades vendidas")

# Detener Spark
spark.stop()
