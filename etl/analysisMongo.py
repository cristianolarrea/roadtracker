import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, countDistinct
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark
import time
import findspark
findspark.init()

def init_spark():        
    return pyspark.sql.SparkSession \
        .builder \
        .appName("RoadTracker") \
        .getOrCreate() \
        .read \
        .format("jdbc") \
        .option("url", "jdbc:redshift://roadtracker.cqgyzrqagvgs.us-east-1.redshift.amazonaws.com:5439/road-tracker?user=admin&password=roadTracker1") \
        .option("dbtable", "vasco") \
        .option("tempdir", "s3n://path/for/temp/data")

def init_mongo():
    #use local
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


mongo = init_mongo()
spark = init_spark()

LastTimestamp = 0

# back in time (1 minute)
backInTime = 60

while True:
    
    dfFull = spark.load()
    dfFull.cache()
    
    # speed_limit as speed_limit 
    dfFull = dfFull.withColumnRenamed("speed_limit", "road_speed")
    dfFull = dfFull.withColumnRenamed("road_id", "road")
    dfFull = dfFull.withColumnRenamed("timestamp", "time")

    # limit time to 1 minute before the last timestamp
    LimitTime = LastTimestamp - backInTime
    print(f'LimitTime: {LimitTime}')
    
    # Filter the records until 1 minute before the last timestamp
    dfNew = dfFull.filter(F.col("timestamp") > LimitTime)

    # get distinct plates
    plates = dfNew.select("plate").distinct()

    # get the last 3 records of each car in plates from dfFull
    dfNewRoad = dfFull.join(plates, "plate", "inner")

    windowDept = Window.partitionBy("plate")\
        .orderBy(col("time").desc())

    # get the last 3 records of each car
    dfNew = dfNew.withColumn("row", row_number().over(windowDept)) \
        .filter(col("row") <= 3)

    print(f'Size of batch: {dfNew.count()}')

    # ############################################
    # --------------- BASE ANALYSIS --------------
    # ############################################

    # -----------------------
    # VELOCIDADE E ACELERACAO
    
    start_time = time.time()

    # calculo da velocidade
    df = dfNew.withColumn("speed", F.col("x") - F.lag("x", -1).over(windowDept))
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

    # ----------------------- PRIORIDADE!
    # ANALISE 6: LISTA DE VEICULOS COM RISCO DE COLISAO
    # Placa e velocidade
    CollisionRisk = df.filter(F.col("collision_risk") == 1) \
        .select("plate", "speed")

    mongo.createDataFrame(CollisionRisk.rdd) \
        .write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis6") \
        .save()
    
    # CollisionRisk.write.format("mongodb") \
    #     .mode("overwrite") \
    #     .option("database", "roadtracker") \
    #     .option("collection", "analysis6") \
    #     .save()


    time_analysis6 = time.time() - start_time
    print(f'Time to analysis 6: {time_analysis6}')
    # -----------------------
    
    
    # -----------------------
    # ANALISE 4: NUMERO DE VEICULOS COM RISCO DE COLISAO
    
    start_time = time.time()
    
    cars_collision_risk = CollisionRisk \
        .count()

    # create a dataframe with the number of cars with collision risk
    cars_collision_risk = mongo.createDataFrame([(cars_collision_risk,)], ['cars_collision_risk'])

    cars_collision_risk.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis4") \
        .save()
        
    time_analysis4 = time.time() - start_time
    print(f'Time to analysis 4: {time_analysis4}')
    # -----------------------


    # -----------------------
    # ANALISE 1: NÚMERO DE RODOVIAS MONITORADAS
    
    start_time = time.time()
    
    n_roads = dfNew.select("road").distinct().count()

    # create a dataframe with the number of roads
    n_roads = mongo.createDataFrame([(n_roads,)], ['n_roads'])

    n_roads.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis1") \
        .save()
        
    time_analysis1 = time.time() - start_time
    print(f'Time to analysis 1: {time_analysis1}')
    # -----------------------


    # -----------------------
    # ANALISE 2: NUMERO TOTAL DE VEICULOS MONITORADOS
    
    start_time = time.time()
    
    n_cars = dfNew.select("plate").distinct().count()

    # create a dataframe with the number of cars
    n_cars = mongo.createDataFrame([(n_cars,)], ['n_cars'])

    n_cars.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis2") \
        .save()
        
    time_analysis2 = time.time() - start_time
    print(f'Time to analysis 2: {time_analysis2}')
    # -----------------------


    # -----------------------
    # ANALISE 5: LISTA DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # Placa, velocidade e se está com risco de colisão
    
    start_time = time.time()
    
    df = df.withColumn("over_speed_limit", F.when(F.col("speed") > F.col("road_speed"), 1).otherwise(0))
    
    OverSpeedLimit = df.filter(F.col("over_speed_limit") == 1) \
        .select("plate", "speed", "collision_risk")

    OverSpeedLimit.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis5") \
        .save()
        
    time_analysis5 = time.time() - start_time
    print(f'Time to analysis 5: {time_analysis5}')
    # -----------------------


    # -----------------------
    # ANALISE 3: NUMERO DE VEICULOS ACIMA DO LIMITE DE VELOCIDADE
    # add a column for the cars over the speed limit
    
    start_time = time.time()
    
    cars_over_speed_limit = OverSpeedLimit.count()

    # create a dataframe with the number of cars over the speed limit
    cars_over_speed_limit = mongo.createDataFrame([(cars_over_speed_limit,)], ['cars_over_speed_limit'])

    cars_over_speed_limit.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "analysis3") \
        .save()
        
    time_analysis3 = time.time() - start_time
    print(f'Time to analysis 3: {time_analysis3}')
    # -----------------------


    # ############################################
    # ---------------- HISTORICAS ----------------
    # ############################################

    # --------------------
    # ANALISE HISTORICA 1: TOP 100 VEICULOS QUE PASSARAM POR MAIS RODOVIAS
    
    start_time = time.time()
    dfRoadCount = dfFull.groupBy("plate").agg(countDistinct('road')).withColumnRenamed("count(road)", "road_count")

    # get the top 100
    dfRoadCount = dfRoadCount.orderBy(col("road_count").desc()).limit(100)

    dfRoadCount.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "historical1") \
        .save()
    
    time_historical1 = time.time() - start_time
    print(f'Time to historical 1: {time_historical1}')
    # --------------------


    # --------------------
    # CALCULO TODAS AS VELOCIDADES
    
    start_time = time.time()
    
    windowDept = Window.partitionBy("plate").orderBy(col("time").desc())
    dfCalcs = dfFull.withColumn("row",row_number().over(windowDept))

    # calc all speeds
    dfCalcs = dfCalcs.withColumn("speed", F.col("x") - F.lag("x", -1).over(windowDept))

    # make all values positive
    dfCalcs = dfCalcs.withColumn("speed", F.abs(F.col("speed")))

    # calc all accs
    dfCalcs = dfCalcs.withColumn("acc", F.col("speed") - F.lag("speed", -1).over(windowDept))

    # drop nulls column
    dfCalcs = dfCalcs.na.drop()
    # --------------------

    # --------------------
    # ANALISE HISTORICA 2: ESTATISTICAS POR RODOVIA
    # get average speed per road
    dfStats = dfCalcs.groupBy("road").avg("speed", "road_size") \
        .withColumnRenamed("avg(speed)", "avg_speed") \
        .withColumnRenamed("avg(road_size)", "road_size")

    # calculate avg time to cross
    dfStats = dfStats.withColumn("avg_time_to_cross", F.col( "road_size") / F.col("avg_speed"))
    
    # select needed columns
    dfStats = dfStats.select("road", "avg_speed", "avg_time_to_cross")
    
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
        
    time_historical2 = time.time() - start_time
    print(f'Time to historical 2: {time_historical2}')
    # --------------------


    # --------------------
    # ANALISE HISTORICA 3: CARROS PROIBIDOS DE CIRCULAR POR DIREÇÃO PERIGOSA
    # partition by plate and order by time (twice to have ascending and descending row numbers)
    
    start_time = time.time()

    # partition by plate and order by time (twice to have ascending and descending row numbers)
    windowDept = Window.partitionBy("plate").orderBy(col("time").asc())

    # create rows columns
    dfSpeeds = dfCalcs.withColumn("row",row_number().over(windowDept))

    # check where speed is greater than road_speed and the previous speed was less than road_speed (that is, new infraction)
    dfSpeeds = dfSpeeds.withColumn("change_in_speed",
                    F.when(((F.col("speed") > F.col("road_speed")) & (F.lag("speed", 1).over(windowDept) <= F.col("road_speed"))) , 1) \
                    .otherwise(0))

    # check for vehicles that enter a road with speed > road_speed (infraction)
    dfSpeeds = dfSpeeds.withColumn("change_in_speed",
                        F.when(((F.col("speed") > F.col("road_speed")) & (F.col("row") ==1)), 1) \
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
        
    time_historical3 = time.time() - start_time
    print(f'Time to historical 3: {time_historical3}')
    # --------------------
    

    # ############################################
    # ----------------- TIMES -------------------
    # ############################################

    # create a dataframe with all times
    dfTimes = mongo.createDataFrame([
        ("analysis1", time_analysis1),
        ("analysis2", time_analysis2),
        ("analysis3", time_analysis3),
        ("analysis4", time_analysis4),
        ("analysis5", time_analysis5),
        ("analysis6", time_analysis6),
        ("historical1", time_historical1),
        ("historical2", time_historical2),
        ("historical3", time_historical3)
    ], ["analysis", "time"])

    dfTimes.write.format("mongodb") \
        .mode("overwrite") \
        .option("database", "roadtracker") \
        .option("collection", "times") \
        .save()

    # Update timestamp
    LastTimestamp = dfFull.select(col("time")).agg({"time": "max"}).collect()[0][0]
    print(f'Last timestamp: {LastTimestamp}')
