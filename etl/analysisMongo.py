import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, countDistinct
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import time

def init_spark():
    # use local
    mongo_conn = "mongodb://127.0.0.1"
    conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
    conf.set("spark.write.connection.uri", mongo_conn)
    conf.set("spark.mongodb.write.database", "roadtracker")
    conf.set("spark.mongodb.write.collection", "collisionRisk")
    
    sc = SparkContext.getOrCreate(conf=conf)
        
    return SparkSession(sc) \
        .builder \
        .appName("RoadTracker") \
        .getOrCreate()

spark = init_spark()

schema = StructType([
    StructField('road', StringType(), nullable=True),
    StructField('road_speed', IntegerType(), nullable=True),
    StructField('road_size', IntegerType(), nullable=True),
    StructField('x', IntegerType(), nullable=True),
    StructField('y', IntegerType(), nullable=True),
    StructField('plate', StringType(), nullable=True),
    StructField('time', FloatType(), nullable=True),
    StructField('direction', IntegerType(), nullable=True)
])

# Create an empty DataFrame with the defined schema
dfLast = spark.createDataFrame([], schema)

while True:
    
    df_original = spark.read \
        .format('mongodb') \
        .option("database", "roadtracker") \
        .option("collection", "sensor-data") \
        .load()

    dfBatch = df_original.subtract(dfLast)
    
    dfLast = dfBatch

    start_time = time.time()

    # -----------------------
    # VELOCIDADE E ACELERACAO
    windowDept = Window.partitionBy("plate").orderBy(col("time").desc())

    df = dfBatch.withColumn("row",row_number().over(windowDept)) \
        .filter(col("row") <= 3)

    # calculo da velocidade
    df = df.withColumn("speed", F.col("x") - F.lag("x", -1).over(windowDept))
    # make all values positive
    df = df.withColumn("speed", F.abs(F.col("speed")))
    # calculo da aceleracao
    df = df.withColumn("acc", F.col("speed") - F.lag("speed", -1).over(windowDept))
    # drop null values
    df = df.na.drop()
    # -----------------------


    # -----------------------
    # DF DE RISCO DE COLISÃO
    windowDept = Window.partitionBy("road", "y").orderBy("x")
    # calcula o risco de colisao fazendo posicao + (velocidade * direcao) + (aceleracao * direcao)
    df = df.withColumn("collision_risk",
                       F.when(F.col("direction") == 1,
                              F.when((F.col("x") + F.col("speed") + F.col("acc")) > (F.lag("x", -1).over(windowDept) + F.lag("speed", -1).over(windowDept) + F.lag("acc",-1).over(windowDept)), 1).otherwise(0)) \
                       .otherwise(F.when((F.col("x") - F.col("speed") - F.col("acc")) < (F.lag("x", 1).over(windowDept) - F.lag("speed", 1).over(windowDept) - F.lag("acc", 1).over(windowDept)), 1).otherwise(0)))
    # -----------------------


    # ----------------------- PRIORIDADE
    # ANALISE 6: LISTA DE VEICULOS COM RISCO DE COLISAO
    # Placa e velocidade
    CollisionRisk = df.filter(F.col("collision_risk") == 1) \
        .select("plate", "speed")

    CollisionRisk.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis6") \
        .save()
    # -----------------------


    # -----------------------
    # ANALISE 1: NÚMERO DE RODOVIAS MONITORADAS
    n_roads = dfBatch.select("road").distinct().count()

    # create a dataframe with the number of roads
    n_roads = spark.createDataFrame([(n_roads,)], ['n_roads'])

    n_roads.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis1") \
        .save()
    # -----------------------


    # -----------------------
    # ANALISE 2: NUMERO TOTAL DE VEICULOS MONITORADOS
    n_cars = dfBatch.select("plate").distinct().count()

    # create a dataframe with the number of cars
    n_cars = spark.createDataFrame([(n_cars,)], ['n_cars'])

    n_cars.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis2") \
        .save()
    # -----------------------


    # -----------------------
    # ANALISE 3: NUMERO DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # add a column for the cars over the speed limit
    df = df.withColumn("over_speed_limit", F.when(F.col("speed") > F.col("road_speed"), 1).otherwise(0))

    cars_over_speed_limit = df.filter(F.col("over_speed_limit") == 1) \
        .select("plate") \
        .distinct() \
        .count()

    # create a dataframe with the number of cars over the speed limit
    cars_over_speed_limit = spark.createDataFrame([(cars_over_speed_limit,)], ['cars_over_speed_limit'])

    cars_over_speed_limit.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis3") \
        .save()
    # -----------------------


    # -----------------------
    # ANALISE 4: NUMERO DE VEICULOS COM RISCO DE COLISAO
    cars_collision_risk = df.filter(F.col("collision_risk") == 1) \
        .select("plate").distinct().count()

    # create a dataframe with the number of cars with collision risk
    cars_collision_risk = spark.createDataFrame([(cars_collision_risk,)], ['cars_collision_risk'])

    cars_collision_risk.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis4") \
        .save()
    # -----------------------


    # -----------------------
    # ANALISE 5: LISTA DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # Placa, velocidade e se está com risco de colisão
    OverSpeedLimit = df.filter(F.col("over_speed_limit") == 1) \
        .select("plate", "speed", "collision_risk")

    OverSpeedLimit.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis5") \
        .save()
    # -----------------------


    # ############################################
    # ---------------- HISTORICAS ----------------
    # ############################################

    # --------------------
    # ANALISE HISTORICA 1: TOP 100 VEICULOS QUE PASSARAM POR MAIS RODOVIAS
    dfRoadCount = df_original.groupBy("plate").agg(countDistinct('road')).withColumnRenamed("count(road)", "road_count")

    # get the top 100
    dfRoadCount = dfRoadCount.orderBy(col("road_count").desc()).limit(100)

    dfRoadCount.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "historical1") \
        .save()
    # --------------------


    # --------------------
    # CALCULO TODAS AS VELOCIDADES
    windowDept = Window.partitionBy("plate").orderBy(col("time").desc())
    dfCalcs = df_original.withColumn("row",row_number().over(windowDept))

    # calc all speeds
    dfCalcs = dfCalcs.withColumn("speed", F.col("x") - F.lag("x", -1).over(windowDept))

    # make all values positive
    dfCalcs = dfCalcs.withColumn("speed", F.abs(F.col("speed")))

    # calc all accs
    dfCalcs = dfCalcs.withColumn("acc", F.col("speed") - F.lag("speed", -1).over(windowDept))

    # drop nulls and row column
    dfCalcs = dfCalcs.na.drop()
    # --------------------


    # --------------------
    # ANALISE HISTORICA 2: ESTATISTICAS POR RODOVIA
    # get average speed per road
    dfStats = dfCalcs.groupBy("road").avg("speed", "road_size") \
        .withColumnRenamed("avg(speed)", "avg_speed") \
        .withColumnRenamed("avg(road_size)", "road_size")

    # calculate avg time to cross
    dfStats = dfStats.withColumn("avg_time_to_cross", F.col( "road_size") / F.col("avg_speed")).drop("road_size")

    # get rows where speed = 0 and acc = 0 (collisions)
    dfCollisions = dfCalcs.filter((F.col("speed") == 0) & (F.col("acc") == 0))

    # group by road and count
    dfCollisions = dfCollisions.groupBy("road").count().withColumnRenamed("count", "total_collisions")

    # join the dataframes to get all stats
    dfStats = dfStats.join(dfCollisions, "road", "left")

    dfStats.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "historical2") \
        .save()
    # --------------------


    # --------------------
    # ANALISE HISTORICA 3: CARROS PROIBIDOS DE CIRCULAR POR DIREÇÃO PERIGOSA
    # partition by plate and order by time (twice to have ascending and descending row numbers)
    windowDept = Window.partitionBy("plate").orderBy(col("time").desc())
    windowDept2 = Window.partitionBy("plate").orderBy(col("time").asc())

    # create rows columns
    dfSpeeds = dfCalcs.withColumn("row",row_number().over(windowDept))
    dfSpeeds = dfSpeeds.withColumn("row2",row_number().over(windowDept2))

    # check where speed is greater than 120 and the previous speed was less than road_speed (that is, new infraction)
    dfSpeeds = dfSpeeds.withColumn("change_in_speed",
                                   F.when(((F.col("speed") > F.col("road_speed")) & (F.lag("speed", -1).over(windowDept) <= F.lag("road_speed", -1).over(windowDept) )) , 1) \
                                   .otherwise(0))

    # check for vehicles that enter a road with speed > road_speed (infraction)
    dfSpeeds = dfSpeeds.withColumn("change_in_speed",
                                   F.when(((F.col("speed") > F.col("road_speed")) & (F.col("row2") ==1)), 1) \
                                   .otherwise(F.col("change_in_speed")))

    # chosen T (change it after testing)
    t = 2500000000

    # get all rows where now() - time < t
    dfSpeeds = dfSpeeds.withColumn("past_time", F.unix_timestamp(F.current_timestamp()).cast("double"))
    dfSpeeds = dfSpeeds.withColumn("diff_time", F.col("past_time") - F.col("time"))
    dfSpeeds = dfSpeeds.filter(F.col("diff_time") < t)

    #  check which cars have more than 10 infractions
    dfInfractions = dfSpeeds.groupBy("plate").sum("change_in_speed") \
        .withColumnRenamed("sum(change_in_speed)", "total_infractions").filter(F.col("total_infractions") >= 1)

    dfInfractions.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "historical3") \
        .save()
    # --------------------


    print("--- %s seconds ---" % (time.time() - start_time))
