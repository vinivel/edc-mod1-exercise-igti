from pyspark.sql.functions import mean,max,min,col,count
from pyspark.sql import SparkSession

spark = (
     SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
    
)

#Ler os dados do ENEM 2020
enem = (
    spark
    .read
    .format("csv")
    .option("header",True)
    .option("inferSchema",True)
    .option("delimiter",";")
    .load("s3://datalake-viniciusveloso-1234/raw-data/MICRODADOS_ENEM_2020.csv")
)

(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year")
    .save("s3://datalake-viniciusveloso-1234/staging/enem")
)