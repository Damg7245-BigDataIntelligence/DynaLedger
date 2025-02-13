import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# API endpoints
API_BASE_URL = "http://localhost:8000"

def check_data_availability(source, year, quarter):
    """Check if data is available in Snowflake"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/check-availability",
            params={"source": source, "year": year, "quarter": quarter}
        )
        return response.json().get("available", False)
    except Exception as e:
        st.error(f"Error checking data availability: {str(e)}")
        return False

def fetch_table_info(data_source):
    """Fetch table information based on the data source"""
    try:
        response = requests.get(f"{API_BASE_URL}/get-table-info", params={"data_source": data_source})
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch table info: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error fetching table info: {str(e)}")
        return None

def fetch_financial_data(year, quarter, data_type, source):
    """Fetch financial data based on the selected parameters"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/get-financial-data",
            params={"year": year, "quarter": quarter, "data_type": data_type, "source": source}
        )
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch data: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None
    
def execute_custom_query(query, data_source):
    """Execute custom query against Snowflake"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/execute-custom-query",
            json={"query": query},
            params={"data_source": data_source}
        )
        if response.status_code == 200:
            return pd.DataFrame(response.json()["data"])
        else:
            st.error(f"Query failed: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="SEC Financial Data Explorer",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("SEC Financial Data Explorer")
    
    # Create tabs for different functionalities
    tab1, tab2 = st.tabs(["Data Explorer", "Custom Query"])
    
    with tab1:
        st.header("Data Explorer")
        
        # Input controls
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            source = st.selectbox(
                "Select Data Source",
                ["RAW", "JSON", "FACT Tables"]
            )
        with col2:
            year = st.selectbox(
                "Select Year",
                range(2009, datetime.now().year + 1)
            )
        with col3:
            quarter = st.selectbox(
                "Select Quarter",
                ["Q1", "Q2", "Q3", "Q4"]
            )
        
        with col4:
            data_type = st.selectbox(
                "Select Data Type",
                ["Balance Sheet", "Income Statement", "Cash Flow"]
            )

        if st.button("Load Data"):
            data = fetch_financial_data(year, quarter, data_type, source)
            if data:
                df = pd.DataFrame(data["data"])
                company_name = data.get("company_name", "Unknown Company")
                symbol = data.get("symbol", "N/A")
                
                if not df.empty:
                    st.subheader(f"{company_name} ({symbol}) - {data_type} for {year} {quarter}")
                    st.dataframe(df)
                    
                    df_aggregated = df.groupby('COMPANY').agg({'VALUE': 'sum'}).reset_index()
                    
                    # Sort data to get top and bottom 10 companies
                    df_sorted = df_aggregated.sort_values(by='VALUE', ascending=False)
                    top_10 = df_sorted.head(10)
                    bottom_10 = df_sorted.tail(10)
                    
                    # Create a bar chart for top 10 companies
                    st.subheader("Top 10 Companies")
                    fig_top = px.bar(top_10, x='COMPANY', y='VALUE', title=f"Top 10 {data_type} Overview")
                    st.plotly_chart(fig_top, use_container_width=True)
                    
                    # Create a bar chart for bottom 10 companies
                    st.subheader("Bottom 10 Companies")
                    fig_bottom = px.bar(bottom_10, x='COMPANY', y='VALUE', title=f"Bottom 10 {data_type} Overview")
                    st.plotly_chart(fig_bottom, use_container_width=True)
                    
                    # Optional: Add a summary graph
                    st.subheader("Summary Graph")
                    summary_df = df.groupby('COMPANY').agg({'VALUE': 'sum'}).reset_index()
                    fig_summary = px.pie(summary_df, values='VALUE', names='COMPANY', title="Company Value Distribution")
                    st.plotly_chart(fig_summary, use_container_width=True)
                else:
                    st.warning("No data available for the selected criteria.")
    
    with tab2:
        st.header("Custom SQL Query")
        
        # Dropdown to select data source
        data_source = st.selectbox("Select Data Source", ["Raw", "JSON"])
        
        # Fetch and display table info
        table_info = fetch_table_info(data_source)
        if table_info:
            num_tables = len(table_info)
            cols = st.columns(num_tables)
            for i, table in enumerate(table_info):
                with cols[i]:
                    st.subheader(f"Table: {table['name']}")
                    st.write("Columns:")
                    st.dataframe(pd.DataFrame(table['columns']), height=390)
                    st.write("Sample Data:")
                    st.dataframe(pd.DataFrame(table['sample_data']), height=150)
        
        # Query input
        query = st.text_area(
            "Enter your SQL query:",
            height=200,
            help="Enter a valid SQL query to execute against the Snowflake database"
        )
        
        if st.button("Execute Query"):
            if query:
                df = execute_custom_query(query, data_source)
                if df is not None:
                    st.success("Query executed successfully!")
                    st.dataframe(df)
            else:
                st.warning("Please enter a query to execute")
                
                
if __name__ == "__main__":
    main()