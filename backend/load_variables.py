from airflow.models import Variable
import json
import os

def load_variables():
    """Load variables into Airflow from JSON file"""
    try:
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Navigate to the variables directory (two levels up)
        variables_path = os.path.join(current_dir, "..", "..", "variables", "variables.json")
        
        print(f"Looking for variables file at: {variables_path}")
        
        if not os.path.exists(variables_path):
            raise FileNotFoundError(f"Variables file not found at {variables_path}")
            
        with open(variables_path) as f:
            variables = json.load(f)
            print("Successfully loaded variables file")
            
        # Set each variable in Airflow
        for key, value in variables.items():
            Variable.set(key, str(value))
            print(f"Set variable: {key} = {value}")
            
    except Exception as e:
        print(f"Error loading variables: {str(e)}")
        raise

if __name__ == "__main__":
    load_variables()