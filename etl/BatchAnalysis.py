import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType

import time

S3_BUCKET = "s3a://roadtracker/"
spark = SparkSession.builder \
    .master("local") \
    .appName("RoadTracker") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


schema = StructType([
    StructField("n_roads", LongType(), nullable=False),
    StructField("n_veiculos", LongType(), nullable=False),
    StructField("n_above_limit", LongType(), nullable=False),
    StructField("n_colision_risk", LongType(), nullable=False),
    StructField("list_above_limit", ArrayType(StringType()), nullable=False),
    StructField("list_colision_risk", ArrayType(StringType()), nullable=False)
])

while True:
    df = spark.read.csv("./all_roads.csv", header=True, inferSchema=True)

    results = []

    start_time = time.time()

    # ANALISE 1: NÚMERO DE RODOVIAS MONITORADAS
    n_roads = df.select("road").distinct().count()
    #print("Number of roads: {}".format(n_roads))
    results.append(int(n_roads))

    # ANALISE 2: NUMERO TOTAL DE VEICULOS MONITORADOS
    n_cars = df.select("plate").distinct().count()
    #print("Number of cars: {}".format(n_cars))
    results.append(int(n_cars))

    # CALCULATE SPEED AND ACCELERATION
    windowDept = Window.partitionBy("plate").orderBy(col("time").desc())

    df = df.withColumn("row",row_number().over(windowDept)) \
        .filter(col("row") <= 3)

    # calculo da velocidade
    df = df.withColumn("speed", F.col("x") - F.lag("x", -1).over(windowDept))
    # make all values positive
    df = df.withColumn("speed", F.abs(F.col("speed")))
    # calculo da aceleracao
    df = df.withColumn("acc", F.col("speed") - F.lag("speed", -1).over(windowDept))
    # drop null values
    df = df.na.drop()
    # drop row column
    # df = df.drop("row")
    #df.show()


    # ANALISE 3: NUMERO DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # add a column for the cars over the speed limit
    df = df.withColumn("over_speed_limit", F.when(F.col("speed") > F.col("road_speed"), 1).otherwise(0))

    cars_over_speed_limit = df.filter(F.col("over_speed_limit") == 1) \
        .select("plate") \
        .distinct() \
        .count()
    #print("Number of cars over the speed limit: {}".format(cars_over_speed_limit))
    results.append(int(cars_over_speed_limit))


    # DF DE RISCO DE COLISÃO
    windowDept = Window.partitionBy("road", "y").orderBy("x")
    # calcula o risco de colisao fazendo posicao + (velocidade * direcao) + (aceleracao * direcao)
    df = df.withColumn("collision_risk",
                       F.when(F.col("direction") == 1,
                              F.when((F.col("x") + F.col("speed") + F.col("acc")) > (F.lag("x", -1).over(windowDept) + F.lag("speed", -1).over(windowDept) + F.lag("acc",-1).over(windowDept)), 1).otherwise(0)) \
                       .otherwise(F.when((F.col("x") - F.col("speed") - F.col("acc")) < (F.lag("x", 1).over(windowDept) - F.lag("speed", 1).over(windowDept) - F.lag("acc", 1).over(windowDept)), 1).otherwise(0)))


    # ANALISE 4: NUMERO DE VEICULOS COM RISCO DE COLISAO
    cars_collision_risk = df.filter(F.col("collision_risk") == 1) \
        .select("plate").distinct().count()
    results.append(int(cars_collision_risk))


    # ANALISE 5: LISTA DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # Placa, velocidade e se está com risco de colisão
    CollectionOverSpeedLimit = df.filter(F.col("over_speed_limit") == 1) \
        .select("plate", "speed", "collision_risk") \
        .collect()
    results.append(["teste"]) #TODO: transform in string the content inside the list returned by collect


    # ANALISE 6: LISTA DE VEICULOS COM RISCO DE COLISAO
    # Placa e velocidade
    CollectionCollisionRisk = df.filter(F.col("collision_risk") == 1) \
        .select("plate", "speed") \
        .collect() #TODO: transform in string the content inside the list returned by collect
    results.append(["teste"])


    print("--- %s seconds ---" % (time.time() - start_time))

    result = spark.createDataFrame([tuple(results)], schema)
    result.write.mode('overwrite').parquet('../results/analysis.parquet')