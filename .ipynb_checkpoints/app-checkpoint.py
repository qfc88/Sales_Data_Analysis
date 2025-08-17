from flask import Flask, request, jsonify, render_template
from joblib import load
import pandas as pd

app = Flask(__name__)

# Load the saved model
model = load('sales_prediction_model.joblib')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get input data
        data = request.json
        
        # Create DataFrame with input data
        input_data = pd.DataFrame({
            'QUANTITYORDERED': [data['quantity']],
            'PRICEEACH': [data['price']],
            'month_of_order': [data['month']],
            'product_average_price': [data['avg_price']],
            'customer_total_orders': [data['total_orders']]
        })
        
        # Make prediction
        prediction = model.predict(input_data)[0]
        
        return jsonify({
            'status': 'success',
            'predicted_sales': round(prediction, 2),
            'sales_category': get_sales_category(prediction)
        })
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

def get_sales_category(sales):
    if sales <= 1000:
        return 'Low'
    elif sales <= 5000:
        return 'Medium'
    else:
        return 'High'

if __name__ == '__main__':
    app.run(debug=True)