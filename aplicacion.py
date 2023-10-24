from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Crear sesión de Spark
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()
sc = spark.sparkContext

# Definir el esquema de datos para las ventas de productos
schema = StructType([
    StructField("product", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True)
])

# Cargar el archivo de texto como un RDD
rdd = sc.textFile("/ruta/al/archivo.txt")

# Convertir cada línea del RDD en una tupla con tres elementos: producto, precio y cantidad
def parse_line(line):
    parts = line.split(",")
    product = parts[0]
    price = float(parts[1])
    quantity = int(parts[2])
    return (product, price, quantity)

parsed_rdd = rdd.map(parse_line)

# Utilizar reduceByKey para calcular el total de ventas por producto
total_ventas_por_producto = parsed_rdd.map(lambda x: (x[0], x[1] * x[2])) \
                                     .reduceByKey(lambda a, b: a + b)

# Utilizar reduce para calcular el total de ventas para toda la tienda
total_ventas_tienda = parsed_rdd.map(lambda x: x[1] * x[2]) \
                               .reduce(lambda a, b: a + b)

# Crear un DataFrame a partir del RDD
df = spark.createDataFrame(parsed_rdd, schema=["product", "price", "quantity"])

# Utilizar Spark SQL para realizar consultas
df.createOrReplaceTempView("ventas")
resultado_sql = spark.sql("""
    SELECT product, SUM(price * quantity) AS total_ventas_por_producto
    FROM ventas
    GROUP BY product
    ORDER BY total_ventas_por_producto DESC
""")

# Encontrar el producto más vendido
producto_mas_vendido = resultado_sql.first()

# Imprimir los resultados
print("Total de ventas por producto:")
resultado_sql.show()

print(f"Total de ventas para toda la tienda: {total_ventas_tienda}")
print(f"Producto más vendido: {producto_mas_vendido['product']} con un total de {producto_mas_vendido['total_ventas_por_producto']} unidades vendidas")

# Detener Spark
spark.stop()
