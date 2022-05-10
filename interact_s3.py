import pandas as pd
import boto3


#Criar cliente para interagir com a AWS S3
s3_client = boto3.client('s3')

s3_client.upload_file("data/MICRODADOS_ENEM_2020.csv","datalake-viniciusveloso-1234","raw-data/MICRODADOS_ENEM_2020.csv")

#s3_client.download_file("datalake-viniciusveloso-1234","data/Leia_Me_Enem_2020.pdf","data/Leia_Me_Enem_2020.pdf")


