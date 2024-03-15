
from pyspark.sql import functions as f
from Dependencies.Spark import start_spark

def main():
  
    spark, log, _ = start_spark(app_name = 'mySpark')

    log.warn('etl_job is up-and-running')

    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    log.warn('etl_job is finished')
    spark.stop()

    return None


def extract_data(spark):
    
    df = spark.read.csv('US_Accidents_March23.csv', header = True, inferSchema = True)

    return df

def null_data(df):

    null = [f.sum(f.when(f.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    df_null = df.agg(*null)
    df_null.show()

    return df_null

def transform_data(df):

    df = df.drop('Source', 'Start_Lat', 'Start_Lng', 'End_Lat', 'End_Lng', 'Distance(mi)', 'Zipcode', 'Airport_Code')
    df = df.withColumns({'Occurrence_day': f.to_date(f.col('Start_Time')), 'Weather_Timestamp': f.to_date(f.col('Weather_Timestamp'))})

    media_clima = df.groupBy('Weather_Timestamp').agg(
    {   
     'Temperature(F)': 'avg',
     'Wind_Chill(F)': 'avg',
     'Humidity(%)': 'avg',
     'Pressure(in)': 'avg',
     'Visibility(mi)': 'avg',
     'Wind_Speed(mph)': 'avg',
     'Precipitation(in)': 'avg'
    }
    ).orderBy('Weather_Timestamp')

    for coluna in media_clima.columns:
        if coluna.startswith('avg'):
            media_clima = media_clima.withColumn(coluna, f.round(f.col(coluna), 2))
    
    df_acidentes = df.groupBy('Occurrence_day', 'State', 'County', 'City', 'Street', 'Severity', 'Wind_Direction',\
                       'Weather_Condition', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',\
                       'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop',\
                       'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight'  
                       ).count().orderBy('Occurrence_day')
    
    df_acidentes = df_acidentes.join(media_clima, on = media_clima['Weather_Timestamp'] == df_acidentes['Occurrence_day'])\
                            .orderBy('Occurrence_day')\
                            .drop('Weather_Timestamp')\
                            .dropna(how = 'any')
    
    return df_acidentes

def load_data(df_acidentes):

    df_acidentes.write.parquet('./Data/acidentes.parquet', mode = 'overwrite')
    
    return None


def create_test_data(spark):

    data = extract_data(spark)

    sample = data.sample(withReplacement = False, fraction = 0.2, seed = 42)
    sample.coalesce(1).write.parquet('./Test/test_data/acidentes', mode = 'overwrite')

    df_tf = transform_data(sample)
    df_tf.coalesce(1).write.parquet('./Test/test_data/acidentes_tf', mode = 'overwrite')

    return None


if __name__ == '__main__':
    main()
    
    
