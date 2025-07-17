"""
main.py - Streamlit app entry point for GA4 Analytics Dashboard
"""
import streamlit as st
import datetime
import json
import pandas as pd
from typing import Optional
from src.bq_client import get_bigquery_client, fetch_analytics_data
from src.dashboard import display_source_dashboard
from src.query import get_analytics_query, get_historical_analytics_query
from src.query_cost import get_bq_cost_usage
from src.sheets_connector import get_config_from_sheet

st.set_page_config(page_title="GA4 Analytics Dashboard", layout="wide")

CACHE_DURATION = 3600  # seconds

# --- Utility Functions ---


@st.cache_data(ttl=CACHE_DURATION)
def load_config() -> dict:
    """Load configuration from Google Sheets."""
    return get_config_from_sheet()

# --- Caching Functions ---

@st.cache_data(ttl=CACHE_DURATION)
def fetch_analytics_df(_client, config) -> pd.DataFrame:
    """Fetch and cache analytics data for filtered websites."""
    all_data = []

    for website in config['websites']:
        suffix = website['suffix']
        website_name = website['website']
        try:
            query = get_analytics_query(suffix)
            data = fetch_analytics_data(_client, query, website_name)
        except Exception as e:
            if "does not match any table" in str(e) and "events_intraday_" in str(e):
                import logging as logger
                logger.warning(
                    f"Intraday table not found for website: {website_name}. Using historical data only.")
                historical_query = get_historical_analytics_query(suffix)
                data = fetch_analytics_data(
                    _client, historical_query, website_name)
            else:
                import logging as logger
                logger.warning(
                    f"Error executing query for website {website_name}: {str(e)}")
                continue
        data['website'] = website_name
        all_data.append(data)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    return pd.DataFrame()


@st.cache_data(ttl=CACHE_DURATION)
def fetch_cost_df(_client) -> Optional[pd.DataFrame]:
    """Fetch and cache BigQuery cost usage data."""
    try:
        cost_query = get_bq_cost_usage()
        cost_df = fetch_analytics_data(_client, cost_query)
        return cost_df
    except Exception as e:
        import logging as logger
        logger.warning(f"Could not fetch BigQuery cost usage: {str(e)}")
        return None


@st.cache_resource
def get_bigquery_client_cached():
    """Cache the BigQuery client resource."""
    return get_bigquery_client()

# --- Refresh Logic ---


def refresh_all_data(_client, config):
    fetch_analytics_df.clear()
    fetch_cost_df.clear()
    load_config.clear()
    return fetch_analytics_df(_client, config), fetch_cost_df(_client)

# --- Main App ---
def main():
    """Main entry point for the Streamlit dashboard app."""
    if 'refresh_clicked' not in st.session_state:
        st.session_state.refresh_clicked = False
    
    client = get_bigquery_client_cached()
    
    # Load config once
    config = load_config()
    
    with st.spinner("Loading data..."):
        if st.session_state.refresh_clicked:
            df, cost_df = refresh_all_data(client, config)
            st.session_state.refresh_clicked = False
        else:
            df = fetch_analytics_df(client, config)
            cost_df = fetch_cost_df(client)
    
    if df.empty:
        st.error("No data available. Please check your configuration.")
        return
    
    try:
        display_source_dashboard(df, cost_df, config)
    except Exception as e:
        st.error(f"Error displaying dashboard: {str(e)}")


if __name__ == "__main__":
    main()
