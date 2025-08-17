import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/WOW/Desktop/scala/project/spark-3.1.1-bin-hadoop3.2"
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, when
from pyspark.sql import Window
from pyspark.sql.functions import count, mean, stddev, min, max, countDistinct, avg

def get_spark_schema():
    """Define the schema for input data"""
    return StructType([
        StructField("ORDERNUMBER", IntegerType(), True),
        StructField("QUANTITYORDERED", IntegerType(), True),
        StructField("PRICEEACH", DoubleType(), True),
        StructField("ORDERLINENUMBER", IntegerType(), True),
        StructField("SALES", DoubleType(), True),
        StructField("ORDERDATE", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("QTR_ID", IntegerType(), True),
        StructField("MONTH_ID", IntegerType(), True),
        StructField("YEAR_ID", IntegerType(), True),
        StructField("PRODUCTLINE", StringType(), True),
        StructField("MSRP", IntegerType(), True),
        StructField("PRODUCTCODE", StringType(), True),
        StructField("DEALSIZE", StringType(), True)
    ])

def add_engineered_features(df):
    """Add engineered features exactly as in training"""
    # Define windows
    year_window = Window.partitionBy('YEAR_ID')
    quarter_window = Window.partitionBy('YEAR_ID', 'QTR_ID')
    month_window = Window.partitionBy('YEAR_ID', 'MONTH_ID')
    product_window = Window.partitionBy('PRODUCTLINE')
    product_month_window = Window.partitionBy('PRODUCTLINE', 'YEAR_ID', 'MONTH_ID')
    product_quarter_window = Window.partitionBy('PRODUCTLINE', 'YEAR_ID', 'QTR_ID')

    # Add time-based features
    df = df.withColumn('yearly_avg_quantity', avg('QUANTITYORDERED').over(year_window)) \
        .withColumn('quarter_avg_quantity', avg('QUANTITYORDERED').over(quarter_window)) \
        .withColumn('monthly_avg_quantity', avg('QUANTITYORDERED').over(month_window))

    # Add product-based features
    df = df.withColumn('product_month_avg', avg('QUANTITYORDERED').over(product_month_window)) \
        .withColumn('product_quarter_avg', avg('QUANTITYORDERED').over(product_quarter_window)) \
        .withColumn('product_avg_quantity', avg('QUANTITYORDERED').over(product_window)) \
        .withColumn('product_total_orders', count('ORDERNUMBER').over(product_window))

    # Add price-related features
    df = df.withColumn('price_to_msrp_ratio', col('PRICEEACH') / col('MSRP')) \
        .withColumn('discount_percentage', (1 - col('PRICEEACH') / col('MSRP')) * 100)

    # Add price level
    df = df.withColumn('price_level', 
                      when(col('price_to_msrp_ratio') <= 0.6, 'Low')
                      .when(col('price_to_msrp_ratio') <= 0.8, 'Medium')
                      .otherwise('High'))
    
    return df

def preprocess_input(data, spark_session):
    """
    Preprocess input data before prediction
    
    Args:
        data (dict): Input data dictionary
        spark_session (SparkSession): Active Spark session
        
    Returns:
        DataFrame: Preprocessed Spark DataFrame
    """
    try:
        if not spark_session:
            raise Exception("No active Spark session provided")
        
        # Create a base DataFrame with required fields
        input_data = {
            "ORDERNUMBER": 1,  # Dummy value
            "QUANTITYORDERED": 0,  # Target variable
            "PRICEEACH": float(data["PRICEEACH"]),
            "ORDERLINENUMBER": 1,  # Dummy value
            "SALES": float(data["PRICEEACH"]) * 1,  # Calculated
            "ORDERDATE": "2024-01-01",  # Dummy value
            "STATUS": "Pending",  # Dummy value
            "QTR_ID": int(data["QTR_ID"]),
            "MONTH_ID": int(data["MONTH_ID"]),
            "YEAR_ID": int(data["YEAR_ID"]),
            "PRODUCTLINE": str(data["PRODUCTLINE"]),
            "MSRP": int(data["MSRP"]),
            "PRODUCTCODE": "DUMMY",  # Dummy value
            "DEALSIZE": str(data["DEALSIZE"])
        }
        
        # Create DataFrame with proper schema
        df = spark_session.createDataFrame([input_data], schema=get_spark_schema())
        
        # Add engineered features
        df = add_engineered_features(df)
        
        # Log DataFrame schema for debugging
        print("\nProcessed DataFrame Schema:")
        df.printSchema()
        print("\nProcessed Data Sample:")
        df.show(1, truncate=False)
        
        return df
    
    except Exception as e:
        print(f"Error preprocessing data: {str(e)}")
        raise