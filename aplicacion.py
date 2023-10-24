from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Crear sesión de Spark
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

# Definir el esquema de datos para las ventas de productos
schema = StructType([
    StructField("product", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True)
])

# Cargar los datos de ventas de productos desde un archivo CSV
data = spark.read.format("csv").option("header", "true").schema(schema).load("/ruta/al/archivo.csv")

# Calcular el total de ventas por producto
data = data.withColumn("total_sales", data["price"] * data["quantity"])

# Convertir las características a un vector denso (precio y cantidad)
assembler = VectorAssembler(inputCols=["price", "quantity"], outputCol="features")
data = assembler.transform(data)

# Crear modelo de regresión lineal y entrenarlo con los datos de ventas
lr = LinearRegression(featuresCol="features", labelCol="total_sales")
model = lr.fit(data)

# Hacer predicciones en los datos existentes
predictions = model.transform(data)

# Mostrar las predicciones y resultados
predictions.select("product", "total_sales", "prediction").show()

# Calcular el total de ventas para toda la tienda
total_store_sales = predictions.select("total_sales").agg({"total_sales": "sum"}).collect()[0][0]
print(f"Total de ventas para toda la tienda: {total_store_sales}")

# Encontrar el producto más vendido
most_sold_product = predictions.orderBy("total_sales", ascending=False).first()
print(f"Producto más vendido: {most_sold_product['product']} con un total de {most_sold_product['total_sales']} unidades vendidas")

# Puedes escribir los resultados a un archivo CSV si es necesario
# predictions.select("product", "price", "quantity", "total_sales", "prediction").write \
#     .option("header", "true") \
#     .csv("/ruta/de/salida")

# Detener Spark
spark.stop()

