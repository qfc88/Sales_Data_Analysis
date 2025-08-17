from flask import Flask
import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/Users/WOW/Desktop/scala/project/spark-3.1.1-bin-hadoop3.2"
import findspark
findspark.init()
from pyspark.sql import SparkSession

def create_app():
    # Initialize Flask app
    app = Flask(__name__)
    
    # Initialize Spark
    findspark.init()
    
    # Configure Spark
    spark = SparkSession.builder \
        .appName("SalesPredictionAPI") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql_2.12:3.1.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    # Register Spark session in app context
    app.spark = spark
    
    # Configuration settings
    app.config.update(
        SECRET_KEY=os.environ.get('SECRET_KEY', 'dev'),
        MODEL_PATH=os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                              'models', 
                              'demand_prediction_model'),  # Updated model path
        DEBUG=True
    )
    
    # Import and register blueprints
    from .views import main_bp
    app.register_blueprint(main_bp)
    
    return app