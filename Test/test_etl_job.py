import unittest
from pyspark.sql import functions as f
from Dependencies.Spark import start_spark
from Job.etl_job import transform_data


class SparkETLTests(unittest.TestCase):

    def setUp(self):

        self.spark, *_ = start_spark()
        self.test_data_path = 'Test/test_data/'

    def tearDown(self):

        self.spark.stop()

    def test_transform_data(self):

        input_data = self.spark.read.parquet(self.test_data_path + 'acidentes')

        expected_data = self.spark.read.parquet(self.test_data_path + 'acidentes_tf')

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = expected_data.agg({   
                                'avg(Temperature(F))': 'avg',
                                'avg(Wind_Chill(F))': 'avg',
                                'avg(Humidity(%))': 'avg',
                                'avg(Pressure(in))': 'avg',
                                'avg(Visibility(mi))': 'avg',
                                'avg(Wind_Speed(mph))': 'avg',
                                'avg(Precipitation(in))': 'avg'
                            }).collect()[0]
        
        expected_avg_steps_rounded = {}
        for col_name, avg_value in expected_avg_steps.asDict().items():
            expected_avg_steps_rounded[col_name] = round(avg_value, 2)
        
        data_transformed = transform_data(input_data)

        cols = len(data_transformed.columns)
        rows = data_transformed.count()
        avg_steps = data_transformed.agg({   
                                'avg(Temperature(F))': 'avg',
                                'avg(Wind_Chill(F))': 'avg',
                                'avg(Humidity(%))': 'avg',
                                'avg(Pressure(in))': 'avg',
                                'avg(Visibility(mi))': 'avg',
                                'avg(Wind_Speed(mph))': 'avg',
                                'avg(Precipitation(in))': 'avg'
                            }).collect()[0]
        
        avg_steps_rounded = {}
        for col_name, avg_value in avg_steps.asDict().items():
            avg_steps_rounded[col_name] = round(avg_value, 2)

        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps_rounded, avg_steps_rounded)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()