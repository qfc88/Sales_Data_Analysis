import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/WOW/Desktop/scala/project/spark-3.1.1-bin-hadoop3.2"
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

def initialize_spark():
    """Initialize Spark session"""
    findspark.init()
    
    return SparkSession.builder \
        .appName("SalesPredictionAPI") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql_2.12:3.1.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def load_spark_model(model_path):
    """
    Load the saved Spark pipeline model
    
    Args:
        model_path (str): Path to the saved model

    Returns:
        PipelineModel: Loaded model
    """
    try:
        # Print the model path for debugging
        print(f"Attempting to load model from path: {model_path}")
        print(f"Does path exist? {os.path.exists(model_path)}")
        
        # Initialize Spark
        spark = initialize_spark()
        
        # Verify model path exists
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found at path: {model_path}")
        
        # Load the pipeline model
        model = PipelineModel.load(model_path)
        
        # Print success message
        print("Model loaded successfully!")
        
        return model
    
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise