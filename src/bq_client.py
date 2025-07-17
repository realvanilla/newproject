from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import pandas as pd
import streamlit as st


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
            )        
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info("BigQuery client created successfully")
        return client
    except FileNotFoundError as e:
        logger.error(f"Service account file not found: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to create BigQuery client: {str(e)}")
        raise

def fetch_analytics_data(client, query,website="uknkown"):
    try:
        logger.info(f"Executing BigQuery query for : {website}...")
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("Query returned no data")
        else:
            logger.info(f"Query returned {len(df)} rows")
            
        return df
    except Exception as e:
        logger.warning(f"Error executing BigQuery query: {str(e)}")
        raise