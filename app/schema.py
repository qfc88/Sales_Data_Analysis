def validate_input(data):
    """Validate input data against required schema"""
    required_fields = {
        'YEAR_ID': int,
        'QTR_ID': int,
        'MONTH_ID': int,
        'PRODUCTLINE': str,
        'MSRP': int,
        'PRICEEACH': (int, float),
        'DEALSIZE': str
    }
    
    # Check if all required fields are present
    if not all(field in data for field in required_fields):
        print(f"Missing fields. Required: {list(required_fields.keys())}, Got: {list(data.keys())}")
        return False
    
    # Validate data types
    try:
        # Type validation
        for field, field_type in required_fields.items():
            if not isinstance(data[field], field_type):
                if field == 'PRICEEACH' and isinstance(data[field], (int, float)):
                    continue
                print(f"Invalid type for {field}. Expected {field_type}, Got {type(data[field])}")
                return False
        
        # Value validation
        if not (1 <= data['QTR_ID'] <= 4):
            print(f"Invalid QTR_ID: {data['QTR_ID']}. Must be between 1 and 4.")
            return False
        
        if not (1 <= data['MONTH_ID'] <= 12):
            print(f"Invalid MONTH_ID: {data['MONTH_ID']}. Must be between 1 and 12.")
            return False
        
        valid_productlines = {
            'Classic Cars', 'Vintage Cars', 'Motorcycles', 
            'Trucks and Buses', 'Planes', 'Ships', 'Trains'
        }
        if data['PRODUCTLINE'] not in valid_productlines:
            print(f"Invalid PRODUCTLINE: {data['PRODUCTLINE']}. Must be one of {valid_productlines}")
            return False
            
        if data['DEALSIZE'] not in {'Small', 'Medium', 'Large'}:
            print(f"Invalid DEALSIZE: {data['DEALSIZE']}. Must be Small, Medium, or Large")
            return False
        
        if data['MSRP'] <= 0:
            print(f"Invalid MSRP: {data['MSRP']}. Must be positive")
            return False
        
        if data['PRICEEACH'] <= 0:
            print(f"Invalid PRICEEACH: {data['PRICEEACH']}. Must be positive")
            return False
            
        return True
        
    except Exception as e:
        print(f"Validation error: {str(e)}")
        return False