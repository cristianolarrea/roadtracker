import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, countDistinct
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
import time

def init_spark():
    # use local
    mongo_conn = "mongodb://127.0.0.1"
    conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
    conf.set("spark.mongodb.write.connection.uri", mongo_conn)
    conf.set("spark.mongodb.write.database", "roadtracker")
    
    sc = SparkContext.getOrCreate(conf=conf)
        
    return SparkSession(sc) \
        .builder \
        .appName("RoadTracker") \
        .getOrCreate()

spark = init_spark()

LastTimeStamp = 0
backInTime = 60

try:
    while True:

        # read from mongodb
        dfFull = spark.read.format("mongodb") \
            .option("database", "roadtracker") \
            .option("collection", "sensor-data") \
            .load() \
            .cache()

        # limit time to 1 minute before the last timestamp
        LimitTime = LastTimeStamp - backInTime
        print(f'LimitTime: {LimitTime}')

        # Filter the records until 1 minute before the last timestamp
        dfRecent = dfFull.filter(F.col("time") > LimitTime)

        # get distinct plates
        plates = dfRecent.select("plate").distinct()

        # filter last 5 minutes of dfFull to get the last 3 records of each car
        dfGet3Registers = dfFull.filter(F.col("time") > (LimitTime - 240))

        # get the last 3 records of each car in plates from dfFull
        dfNew = dfGet3Registers.join(plates, "plate", "inner")

        windowDept = Window.partitionBy("plate") \
            .orderBy(col("time").desc())
        
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

        CollisionRisk.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "roadtracker") \
            .option("collection", "analysis6") \
            .save()

        time_analysis6 = time.time() - start_time
        print(f'Time to analysis 6: {time_analysis6}')
        # -----------------------


        # -----------------------
        # ANALISE 4: NUMERO DE VEICULOS COM RISCO DE COLISAO

        start_time = time.time()

        cars_collision_risk = CollisionRisk \
            .count()

        # create a dataframe with the number of cars with collision risk
        cars_collision_risk = spark.createDataFrame([(cars_collision_risk,)], ['cars_collision_risk'])
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
        n_roads = spark.createDataFrame([(n_roads,)], ['n_roads'])

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
        n_cars = spark.createDataFrame([(n_cars,)], ['n_cars'])

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
        cars_over_speed_limit = spark.createDataFrame([(cars_over_speed_limit,)], ['cars_over_speed_limit'])

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
        # ANALISE HISTORICA 3: CARROS PROIBIDOS DE CIRCULAR
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

        # chosen T (change it to desired T)
        t = 2500000000

        # get all rows where now() - time < t
        dfSpeeds = dfSpeeds.withColumn("past_time", F.unix_timestamp(F.current_timestamp()).cast("double"))
        dfSpeeds = dfSpeeds.withColumn("diff_time", F.col("past_time") - F.col("time"))
        dfSpeeds = dfSpeeds.filter(F.col("diff_time") < t)

        #  check which cars have more than 1 infractions (change to desired number (10))
        dfInfractions = dfSpeeds.groupBy("plate").sum("change_in_speed") \
            .withColumnRenamed("sum(change_in_speed)", "total_infractions").filter(F.col("total_infractions") >= 1) #10

        dfInfractions.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "roadtracker") \
            .option("collection", "historical3") \
            .save()

        time_historical3 = time.time() - start_time
        print(f'Time to historical 3: {time_historical3}')
        # --------------------


        # --------------------
        # ANALISE ALTERNATIVA: CARROS COM DIREÇÃO PERIGOSA

        start_time = time.time()

        windowDept = Window.partitionBy("plate").orderBy(col("time").desc())

        # filter rows with between t1 and t2, setting time t (UPDATE T1 AND T2)
        t1 = 1680000000
        t2 = 1690000000
        dfCalcs = dfCalcs.filter((F.col("time") > t1) & (F.col("time") < t2))

        # create column "changed_y" that is 1 if the car changed y and 0 otherwise
        dfCalcs = dfCalcs.withColumn("changed_lane",
                            F.when(((F.col("y") != F.lag("y", -1).over(windowDept))), 1) \
                                .otherwise(0))

        # create column "over_road_speed" that is 1 if the car is over the road speed and 0 otherwise
        dfCalcs = dfCalcs.withColumn("over_road_speed",
                                F.when(((F.col("speed") > F.col("road_speed"))), 3) \
                                .otherwise(0))

        # create column "over_acc" that is 1 if (acc>40 or acc<-40) and 0 otherwise
        dfCalcs = dfCalcs.withColumn("over_acc",
                                F.when(((F.col("acc") > 40) | (F.col("acc") < -40)), 3) \
                                .otherwise(0))

        # create a column "total" that is the sum of the previous columns for each car, and filter out no infraction cars
        dfCalcs = dfCalcs.withColumn("total", F.col("changed_lane") + F.col("over_road_speed") + F.col("over_acc")).filter(F.col("total") > 0)

        # do sliding window of I seconds 
        # replace 60 with desired I (interval)
        slidingWindow = Window.partitionBy("plate").orderBy(F.col("time").asc()).rangeBetween(Window.currentRow, 60)

        # check if several infractions happened in the interval (7 or more)
        dfCalcs = dfCalcs.withColumn("several_infractions", F.sum("total").over(slidingWindow) >=7).filter(F.col("several_infractions") == True)
        dfSeveralInfractions = dfCalcs.select("plate")

        dfSeveralInfractions.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "roadtracker") \
            .option("collection", "alternative") \
            .save()
        
        time_alternative = time.time() - start_time
        print(f'Time to alternative: {time_alternative}')
        # --------------------


        # ############################################
        # ----------------- TIMES -------------------
        # ############################################

        # create a dataframe with all times
        dfTimes = spark.createDataFrame([
            ("analysis1", time_analysis1),
            ("analysis2", time_analysis2),
            ("analysis3", time_analysis3),
            ("analysis4", time_analysis4),
            ("analysis5", time_analysis5),
            ("analysis6", time_analysis6),
            ("historical1", time_historical1),
            ("historical2", time_historical2),
            ("historical3", time_historical3),
            ("alternative", time_alternative)
        ], ["analysis", "time"])

        dfTimes.write.format("mongodb") \
            .mode("overwrite") \
            .option("database", "roadtracker") \
            .option("collection", "times") \
            .save()
        
        # gets the new timestamp
        LastTimeStamp_new = dfRecent.select(col("time")).agg({"time": "max"}).collect()[0][0]
        
        # if the new timestamp is the same as the previous one, it means there is no new data
        if LastTimeStamp_new == LastTimeStamp:
            # sums 60 to avoid going backInTime = 60
            LastTimeStamp = LastTimeStamp_new + backInTime
            print("No new data.")
        elif LastTimeStamp_new == None:
            LastTimeStamp += backInTime
        else:
            LastTimeStamp = LastTimeStamp_new
            print(f"New data found. Last timestamp: {LastTimeStamp}")


except KeyboardInterrupt:
    print('Interrupted')
    sys.exit(0)