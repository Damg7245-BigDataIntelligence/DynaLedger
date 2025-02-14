from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import snowflake.connector
import os
import numpy as np
from pydantic import BaseModel

app = FastAPI()
load_dotenv()


def get_snowflake_connection(schema_name: str):
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=schema_name  # Set schema dynamically
        )
        print(f"Successfully connected to Snowflake with schema {schema_name}.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database connection error")


class QueryModel(BaseModel):
    query: str
    
    
def sanitize_float_values(data):
    """Convert special float values to None."""
    for item in data:
        for key, value in item.items():
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                item[key] = None
    return data


@app.get("/check-availability")
async def check_data_availability(source: str, year: int, quarter: str):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Query to check if data exists for the given parameters
        query = f"""
        SELECT COUNT(*) 
        FROM sec_tag_data_raw 
        WHERE source_file = '{year}Q{quarter.replace("Q", "")}'
        """
        
        cur.execute(query)
        count = cur.fetchone()[0]
        
        return {"available": count > 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.get("/get-table-info")
async def get_table_info(data_source: str, year: int, quarter: str):
    try:
        schema_name = "SEC_DATA_RAW" if data_source == "Raw" else "SEC_DATA_JSON"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
        
        stage_name = f"{year}Q{quarter.replace('Q', '')}"
        if data_source == "Raw":
            tables = [f"RAW_NUM_{stage_name}", f"RAW_PRE_{stage_name}", f"RAW_SUB_{stage_name}", f"RAW_TAG_{stage_name}"]
        elif data_source == "JSON":
            tables = ["sec_json_data"]
        else:
            raise HTTPException(status_code=400, detail="Invalid data source")
        
        table_info = []
        for table in tables:
            result = cur.execute(f"DESCRIBE TABLE {schema_name}.{table}")
            columns = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
            
            cur.execute(f"SELECT * FROM {schema_name}.{table} LIMIT 3")
            sample_data = [dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]
            
            table_info.append({"name": table, "columns": columns, "sample_data": sample_data})
        
        return table_info
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch table info")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
@app.post("/execute-custom-query")
async def execute_custom_query(query_model: QueryModel, data_source: str):
    try:
        schema_name = "SEC_DATA_RAW" if data_source == "Raw" else "SEC_DATA_JSON"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
        query = query_model.query
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        # Sanitize float values
        sanitized_results = sanitize_float_values(results)
        return {"data": sanitized_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to execute query")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
                        
@app.get("/get-financial-data")
async def get_financial_data(year: int, quarter: str, data_type: str, source: str):
    try:
        schema_name = "SEC_DATA_RAW" if source == "RAW" else "SEC_DATA_JSON"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
        
        # Determine the view to query based on data_type
        view_name = {
            "Balance Sheet": f"{schema_name}.view_balance_sheet",
            "Income Statement": f"{schema_name}.view_income_statement",
            "Cash Flow": f"{schema_name}.view_cash_flow"
        }.get(data_type)
        
        if not view_name:
            raise HTTPException(status_code=400, detail="Invalid data type")
        
        # Construct the query
        query = f"""
        SELECT * FROM {view_name}
        WHERE year = {year} AND quarter = '{quarter}'
        """
        
        print(f"Executing query: {query}")
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        
        sanitized_results = sanitize_float_values(results)
        
        return {"data": sanitized_results}
    except Exception as e:
        print(f"Error executing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch data from the database")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
                        
@app.get("/query-data")
async def execute_query(query: str = Query(..., min_length=1)):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Execute the query
        cur.execute(query)
        
        # Fetch column names
        columns = [desc[0] for desc in cur.description]
        
        # Fetch all rows
        rows = cur.fetchall()
        
        # Convert to list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]
        
        
        sanitized_results = sanitize_float_values(results)
        
        return {"data": sanitized_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()