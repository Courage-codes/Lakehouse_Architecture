import json

def lambda_handler(event, context):
    """
    Simple string replacement function for Step Functions
    
    Expected input:
    {
        "input": "string-to-process",
        "find": "-",
        "replace": "_"
    }
    
    Returns:
    {
        "statusCode": 200,
        "result": "string_to_process",
        "original_input": "string-to-process"
    }
    """
    
    try:
        # Extract parameters from the event
        input_string = event.get('input', '')
        find_string = event.get('find', '')
        replace_string = event.get('replace', '')
        
        # Perform the string replacement
        result = input_string.replace(find_string, replace_string)
        
        # Return the result including original input
        return {
            'statusCode': 200,
            'result': result,
            'original_input': input_string  # Include original input for reference
        }
        
    except Exception as e:
        # Handle any errors
        return {
            'statusCode': 400,
            'error': str(e),
            'result': None,
            'original_input': event.get('input', '')
        }

# Test the function locally (optional)
if __name__ == "__main__":
    test_event = {
        "input": "user-profiles-data",
        "find": "-",
        "replace": "_"
    }
    
    result = lambda_handler(test_event, None)
    print(f"Test result: {result}")
    # Expected output: {'statusCode': 200, 'result': 'user_profiles_data', 'original_input': 'user-profiles-data'}