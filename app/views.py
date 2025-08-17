from flask import Blueprint, render_template, request, jsonify, current_app
from .model_loader import load_spark_model
from .utils import preprocess_input
from .schema import validate_input
import traceback
import logging

# Create blueprint
main_bp = Blueprint('main', __name__)

# Store model globally
model = None

def get_model():
    """Get or load the model"""
    global model
    if model is None:
        try:
            model_path = current_app.config.get('MODEL_PATH')
            if not model_path:
                raise ValueError("MODEL_PATH not configured in app settings")
            model = load_spark_model(model_path)
        except Exception as e:
            current_app.logger.error(f"Error loading model: {str(e)}\n{traceback.format_exc()}")
            raise
    return model

@main_bp.route('/')
def home():
    """Home page route"""
    try:
        return render_template('index.html')
    except Exception as e:
        current_app.logger.error(f"Error rendering home page: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'error': 'Internal server error'
        }), 500

@main_bp.route('/predict', methods=['GET', 'POST'])
def predict():
    """Prediction route"""
    if request.method == 'GET':
        try:
            return render_template('predict.html')
        except Exception as e:
            current_app.logger.error(f"Error rendering prediction page: {str(e)}")
            return jsonify({
                'status': 'error',
                'error': 'Error loading prediction page'
            }), 500
    
    try:
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'error': 'No data provided'
            }), 400
        
        current_app.logger.info(f"Received data: {data}")
        
        # Validate input data
        if not validate_input(data):
            return jsonify({
                'status': 'error',
                'error': 'Invalid input data format'
            }), 400
        
        # Get the model
        model = get_model()
        
        # Get Spark session from app context
        spark = current_app.spark
        
        # Preprocess the input data with Spark session
        processed_data = preprocess_input(data, spark)
        
        # Make prediction
        prediction = model.transform(processed_data)
        
        # Extract prediction value
        prediction_value = prediction.select('prediction').collect()[0][0]
        
        return jsonify({
            'status': 'success',
            'prediction': round(prediction_value, 2),
            'message': f'Predicted quantity: {round(prediction_value, 2)}'
        })
        
    except Exception as e:
        error_msg = f"Error making prediction: {str(e)}\n{traceback.format_exc()}"
        current_app.logger.error(error_msg)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@main_bp.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        model_loaded = model is not None
        return jsonify({
            'status': 'healthy',
            'model_loaded': model_loaded,
            'spark_session': hasattr(current_app, 'spark')
        })
    except Exception as e:
        error_msg = f"Error in health check: {str(e)}\n{traceback.format_exc()}"
        current_app.logger.error(error_msg)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Add logging handler to blueprint
@main_bp.before_app_first_request
def setup_logging():
    if not current_app.debug:
        # Add logging handler
        file_handler = logging.FileHandler('app.log')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s '
            '[in %(pathname)s:%(lineno)d]'
        ))
        current_app.logger.addHandler(file_handler)
    current_app.logger.setLevel(logging.INFO)