import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import json
from datetime import datetime, timedelta


@st.cache_data(ttl=3600)
def get_cached_data(df):
    return df


def display_source_dashboard(df, cost_df, config):
    st.markdown("""
    <style>
    .stApp {
        background-color: black;
    }
    h3 {
                padding: 0.75rem 0px 0.2rem !important;
    }
    h1 {
                padding: 0rem 0px 1rem !important;
    }
    .block-container { padding-top: 3rem !important; padding-bottom: 0rem !important; }
    .compact-header { font-size:20px; font-weight:bold; margin:0; padding:0; }
    .compact-value { font-size:18px; font-weight:bold; margin:0; padding-bottom:15px; }
    .site-title { font-size:22px; font-weight:bold; margin:0; padding-bottom:10px; color:white; }
    .country-line { display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid #4D9DE0; color:white; }
    .country-name { text-align:left; }
    .country-value { text-align:right; }

    /* Custom styling for multiselect */
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #4D9DE0 !important;
    }
    .stMultiSelect [data-baseweb="tag"] span {
        color: white !important;
    }

    /* Align the filter and button */
    div[data-testid="column"] [data-testid="stVerticalBlock"] {
        display: flex;
        align-items: flex-end;
    }
    </style>
    """, unsafe_allow_html=True)
    df = get_cached_data(df)
    if 'minutes_past' not in df.columns:
        st.error("Required column 'minutes_past' not found in data")
        return False
    if 'event_date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['event_date']):
        df['event_date'] = pd.to_datetime(df['event_date'])

    # Get list of websites from the dataframe
    yesterday = pd.Timestamp.now().date() - pd.Timedelta(days=1)
    yesterday_df = df[df['event_date'].dt.date == yesterday]
    last_week = pd.Timestamp.now().date() - pd.Timedelta(days=7)
    websites = (
        df[df['event_date'].dt.date >= last_week]
        .groupby('website')['sessions']
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    # For now, show all websites by default (we'll add the filter at the bottom)
    selected_websites = websites
    filtered_df = df[df['website'].isin(selected_websites)]

    # Calculate overall statistics for all selected websites
    today = pd.Timestamp.now().date()
    yesterday = today - pd.Timedelta(days=1)

    # Total visitors yesterday
    yesterday_df = filtered_df[filtered_df['event_date'].dt.date == yesterday]
    total_yesterday = int(yesterday_df['sessions'].sum())

    # Total visitors today
    today_df = filtered_df[filtered_df['event_date'].dt.date == today]
    total_today = int(today_df['sessions'].sum())    # Total visitors in last 30 minutes
    recent_df = filtered_df[filtered_df['minutes_past'] <= 30]
    # Create website stats for each metric
    total_recent = int(recent_df['sessions'].sum())
      # Calculate monetized totals (websites with monetization = ACTIVE only)
    monetized_websites = [site['website'] for site in config['websites'] 
                         if site['monetization'] == 'ACTIVE']
    
    # Filter data for monetized websites only
    monetized_yesterday_df = yesterday_df[yesterday_df['website'].isin(monetized_websites)]
    total_monetized_yesterday = int(monetized_yesterday_df['sessions'].sum())
    
    monetized_today_df = today_df[today_df['website'].isin(monetized_websites)]
    total_monetized_today = int(monetized_today_df['sessions'].sum())
    
    # Calculate monetized traffic by account for yesterday
    monetized_by_account_yesterday = {}
    for site in config['websites']:
        if site['monetization'] == 'ACTIVE':
            website_name = site['website']
            account = site.get('account', 'Unknown')
            website_sessions = yesterday_df[yesterday_df['website'] == website_name]['sessions'].sum()
            if account in monetized_by_account_yesterday:
                monetized_by_account_yesterday[account] += int(website_sessions)
            else:
                monetized_by_account_yesterday[account] = int(website_sessions)
    
    # Calculate monetized traffic by account for today
    monetized_by_account_today = {}
    for site in config['websites']:
        if site['monetization'] == 'ACTIVE':
            website_name = site['website']
            account = site.get('account', 'Unknown')
            website_sessions = today_df[today_df['website'] == website_name]['sessions'].sum()
            if account in monetized_by_account_today:
                monetized_by_account_today[account] += int(website_sessions)
            else:
                monetized_by_account_today[account] = int(website_sessions)
    
    # Create tooltip text for monetized breakdown
    tooltip_text = "Monetized Traffic by Account:\\n"
    for account, sessions in sorted(monetized_by_account_yesterday.items(), key=lambda x: x[1], reverse=True):
        tooltip_text += f"• {account}: {sessions:,}\\n"
    yesterday_by_website = yesterday_df.groupby('website')['sessions'].sum(
    ).reset_index().sort_values('sessions', ascending=False)
    today_by_website = today_df.groupby('website')['sessions'].sum(
    ).reset_index().sort_values('sessions', ascending=False)

    # For recent data, ensure all websites are shown (even with 0 sessions)
    recent_by_website = recent_df.groupby(
        'website')['sessions'].sum().reset_index()

    # Create a dataframe with all websites and merge with recent data
    all_websites_df = pd.DataFrame({'website': selected_websites})
    recent_by_website = all_websites_df.merge(
        recent_by_website, on='website', how='left').fillna({'sessions': 0})
    recent_by_website['sessions'] = recent_by_website['sessions'].astype(int)
    recent_by_website = recent_by_website.sort_values(
        'sessions', ascending=False)    # Create unified website table
    st.markdown("<div style='margin-top:30px'></div>", unsafe_allow_html=True)# Prepare unified table data
    websites_data = []    # Calculate 5-day data (excluding today and yesterday) - 5 days before yesterday
    end_date = pd.Timestamp.now().date() - pd.Timedelta(days=2)  # Day before yesterday
    start_date = end_date - pd.Timedelta(days=4)  # 5 days back from day before yesterday
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    last_5_days_df = filtered_df[filtered_df['event_date'].dt.date >= start_date]
    last_5_days_df = last_5_days_df[last_5_days_df['event_date'].dt.date <= end_date]
    
    # Create the same pivot table as in website visitors section
    daily_visitors = (
        last_5_days_df
        .groupby(['website', last_5_days_df['event_date'].dt.date])['sessions']
        .sum()
        .reset_index()
    )
    
    # Pivot the data
    visitors_pivot = daily_visitors.pivot(
        index='website', columns='event_date', values='sessions')
    
    # Ensure all 5 days are present as columns (reversed order)
    for date in reversed(date_range):
        if date.date() not in visitors_pivot.columns:
            visitors_pivot[date.date()] = 0
    visitors_pivot = visitors_pivot.reindex(
        sorted(visitors_pivot.columns, reverse=True), axis=1).fillna(0)
    
    # Format column names to show only date in DD/MM format
    date_columns = [pd.Timestamp(col).strftime('%d/%m') for col in visitors_pivot.columns]
    visitors_pivot.columns = date_columns
    visitors_pivot = visitors_pivot.astype(int)
    
    for website_config in config['websites']:
        website_name = website_config['website']
        
        # Get yesterday data
        yesterday_sessions = yesterday_by_website[yesterday_by_website['website'] == website_name]['sessions'].iloc[0] if not yesterday_by_website[yesterday_by_website['website'] == website_name].empty else 0
        
        # Get today data  
        today_sessions = today_by_website[today_by_website['website'] == website_name]['sessions'].iloc[0] if not today_by_website[today_by_website['website'] == website_name].empty else 0
        
        # Get last 30 minutes data
        recent_sessions = recent_by_website[recent_by_website['website'] == website_name]['sessions'].iloc[0] if not recent_by_website[recent_by_website['website'] == website_name].empty else 0
          # Get individual day data from the pivot table
        row_data = {
            'Number': website_config['number'],
            'Website': website_name,
            'Monetization': website_config['monetization'],
            'Account': website_config.get('account', ''),
            'Yesterday': yesterday_sessions,
            'Today': today_sessions,
            '30 Min': recent_sessions
        }
        
        # Add individual date columns
        if website_name in visitors_pivot.index:
            for date_col in date_columns:
                row_data[date_col] = visitors_pivot.loc[website_name, date_col]
        else:
            # If website not in pivot (no data), fill with 0s
            for date_col in date_columns:
                row_data[date_col] = 0
        
        websites_data.append(row_data)

    # Create DataFrame and display with custom HTML table
    websites_performance_df = pd.DataFrame(websites_data)
      # Create custom HTML table function for unified table
    def create_unified_html_table(df, all_df):        # Create account name to color mapping
        unique_accounts = all_df['Account'].unique() if 'Account' in all_df.columns else []
        account_color_map = {}
        for i, account in enumerate(unique_accounts):
            if account and account.strip():  # Only map non-empty accounts
                # Use specific colors for named accounts
                if account == 'Anas':
                    account_color_map[account] = 'account-name-anas'
                elif account == 'Achraf':
                    account_color_map[account] = 'account-name-achraf'
                elif account == 'Ouss2':
                    account_color_map[account] = 'account-name-ouss2'
                else:
                    account_color_map[account] = f"account-name-{(i % 5) + 1}"
        
        html = """
        <style>        .large-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 24px !important;
            background-color: transparent;
            color: white;
            border: none;
        }
        .table-container {
            border-right: 2px solid #4D9DE0;
            border-bottom: 2px solid #4D9DE0;
        }
        .large-table th {
            background-color: #262730;
            color: white;
            font-size: 26px !important;
            font-weight: bold;
            padding: 15px 8px;
            text-align: center;
            border: 1px solid #4D9DE0;
        }
        .large-table th.border-left {
            border-left: 4px solid #4D9DE0 !important;
        }
        .large-table td {
            font-size: 24px !important;
            padding: 12px 8px;
            text-align: center;
            border: 1px solid #4D9DE0;
            background-color: #0e1117;
        }
        .large-table td.no-border {
            border: none !important;
            background-color: black !important;
        }
        .large-table tbody tr td.no-border {
            border: none !important;
            border-top: none !important;
            border-left: none !important;
            border-right: none !important;
            border-bottom: none !important;
        }
        .large-table tbody tr td.no-border:first-child {
            border-left: none !important;
        }
        .large-table tbody tr td.no-border:last-child {
            border-right: none !important;
        }
        /* Override table border for no-border cells */
        .large-table tbody tr:first-child td.no-border:first-child,
        .large-table tbody tr:nth-child(2) td.no-border:first-child,
        .large-table tbody tr:nth-child(3) td.no-border:first-child,
        .large-table tbody tr:nth-child(4) td.no-border:first-child,
        .large-table tbody tr:nth-child(5) td.no-border:first-child,
        .large-table tbody tr:nth-child(6) td.no-border:first-child {
            border-left: none !important;
            box-shadow: none !important;
        }
        .large-table td:nth-child(4) {
            border-left: 2px solid #4D9DE0 !important;
            border-right: 4px solid #4D9DE0 !important;
        }
        .large-table th:nth-child(4) {
            border-left: 2px solid #4D9DE0 !important;
            border-right: 4px solid #4D9DE0 !important;
        }
        /* Add border after 30 Min column (7th column) */
        .large-table td:nth-child(7) {
            border-right: 4px solid #4D9DE0 !important;
        }
        .large-table th:nth-child(7) {
            border-right: 4px solid #4D9DE0 !important;
        }
        /* Add top border starting from Account column */
        .large-table tbody tr:first-child td:nth-child(n+4) {
            border-top: 2px solid #4D9DE0 !important;
        }
        .large-table tr:nth-child(even) td {
            background-color: #1a1d29;
        }
        .monetized-active {
            color: #28a745;
            font-weight: bold;
            font-size: 20px;
        }
        .monetized-review {
            color: #ffc107;
            font-weight: bold;
            font-size: 20px;
        }        .monetized-ready {
            color: #000000;
            font-weight: bold;
            font-size: 20px;
        }
        .monetized-none {
            color: #6c757d;
            font-weight: bold;
            font-size: 18px;
        }        .website-name {
            font-weight: bold !important;
        }        .account-name {
            font-weight: bold !important;
        }
        .account-name-1 { color: #4D9DE0; font-size: 20px; }  /* Blue */
        .account-name-2 { color: #6f42c1; font-size: 20px; }  /* Purple */
        .account-name-3 { color: #fd7e14; font-size: 20px; }  /* Orange */        
        .account-name-4 { color: #17a2b8; font-size: 20px; }  /* Cyan */
        .account-name-5 { color: #28a745; font-size: 20px; }  /* Forest Green */
        .account-name-anas { color: #7a598b; font-size: 20px; }  /* Dark Purple for Anas */
        .account-name-achraf { color: #88977a; font-size: 20px; }  /* Green-Gray for Achraf */
        .account-name-ouss2 { color: #ffc8aa; font-size: 20px; }  /* Light Orange for Ouss2 */
        </style>
        <div class="table-container">
        <table class="large-table">
        <tbody>
        """
        
        # Add total row first (before headers)
        cols = all_df.columns.tolist()
        html += '<tr style="background-color: #262730; font-weight: bold;">'
        for j, col in enumerate(cols):
            css_class = ""
            border_bottom_style = "border-bottom: 2px solid #4D9DE0;"
            
            if col == 'Yesterday':
                css_class = ' class="border-left"'
            elif j > 0 and cols[j-1] == '30 Min':
                css_class = ' class="border-left"'
            
            if col in ['Number', 'Website', 'Monetization', 'Account']:
                if col == 'Account':
                    html += f'<td{css_class} style="font-weight: bold; color: #4D9DE0; {border_bottom_style}">TOTAL</td>'
                else:
                    # No borders for empty columns
                    if css_class:
                        css_class = css_class.replace('"', ' no-border"')
                    else:
                        css_class = ' class="no-border"'
                    html += f'<td{css_class}></td>'
            else:
                # Sum numeric columns (Yesterday, Today, 30 Min, date columns)
                total_val = all_df[col].sum() if col in all_df.columns else 0
                html += f'<td{css_class} style="font-weight: bold; {border_bottom_style}">{total_val:,}</td>'
        html += '</tr>'
        
        # Add total monetized row (only ACTIVE monetization websites)
        monetized_df = all_df[all_df['Monetization'] == 'ACTIVE']
        html += '<tr style="background-color: #0f7c39;">'
        for j, col in enumerate(cols):
            css_class = ""
            border_bottom_style = "border-bottom: 2px solid #4D9DE0;"
            
            if col == 'Yesterday':
                css_class = ' class="border-left"'
            elif j > 0 and cols[j-1] == '30 Min':
                css_class = ' class="border-left"'
            
            if col in ['Number', 'Website', 'Monetization', 'Account']:
                if col == 'Account':
                    html += f'<td{css_class} style="color: white; {border_bottom_style}">TOTAL MONETIZED</td>'
                else:
                    # No borders for empty columns
                    if css_class:
                        css_class = css_class.replace('"', ' no-border"')
                    else:
                        css_class = ' class="no-border"'
                    html += f'<td{css_class}></td>'
            else:
                # Sum numeric columns only for monetized websites
                monetized_val = monetized_df[col].sum() if col in monetized_df.columns and not monetized_df.empty else 0
                html += f'<td{css_class} style="color: white; {border_bottom_style}">{monetized_val:,}</td>'
        html += '</tr>'
        
        # Add total not monetized row (websites that are not ACTIVE)
        not_monetized_df = all_df[all_df['Monetization'] != 'ACTIVE']
        html += '<tr style="background-color: #7c3d0f;">'
        for j, col in enumerate(cols):
            css_class = ""
            border_bottom_style = "border-bottom: 2px solid #4D9DE0;"
            
            if col == 'Yesterday':
                css_class = ' class="border-left"'
            elif j > 0 and cols[j-1] == '30 Min':
                css_class = ' class="border-left"'
            
            if col in ['Number', 'Website', 'Monetization', 'Account']:
                if col == 'Account':
                    html += f'<td{css_class} style="color: white; {border_bottom_style}">NOT MONETIZED</td>'
                else:
                    # No borders for empty columns
                    if css_class:
                        css_class = css_class.replace('"', ' no-border"')
                    else:
                        css_class = ' class="no-border"'
                    html += f'<td{css_class}></td>'
            else:
                # Sum numeric columns only for non-monetized websites
                not_monetized_val = not_monetized_df[col].sum() if col in not_monetized_df.columns and not not_monetized_df.empty else 0
                html += f'<td{css_class} style="color: white; {border_bottom_style}">{not_monetized_val:,}</td>'
        html += '</tr>'
        
        # Add individual account breakdown rows (only for ACTIVE monetization websites)
        if not monetized_df.empty:
            # Get unique accounts from monetized websites
            unique_accounts = monetized_df['Account'].unique()
            # Filter out empty accounts and sort by total traffic
            valid_accounts = [acc for acc in unique_accounts if acc and acc.strip()]
            
            # Calculate total traffic per account to sort them
            account_totals = {}
            for account in valid_accounts:
                account_df = monetized_df[monetized_df['Account'] == account]
                # Use Yesterday column as primary sort metric
                account_total = account_df['Yesterday'].sum() if 'Yesterday' in account_df.columns else 0
                account_totals[account] = account_total
            
            # Sort accounts by total traffic (descending)
            sorted_accounts = sorted(valid_accounts, key=lambda x: account_totals.get(x, 0), reverse=True)
            
            for account in sorted_accounts:
                account_df = monetized_df[monetized_df['Account'] == account]
                # Get account color
                account_color_class = account_color_map.get(account, 'account-name-1')
                account_color = {
                    'account-name-1': '#4D9DE0',
                    'account-name-2': '#6f42c1', 
                    'account-name-3': '#fd7e14',
                    'account-name-4': '#17a2b8',
                    'account-name-5': '#28a745',
                    'account-name-anas': '#7a598b',
                    'account-name-achraf': '#88977a',
                    'account-name-ouss2': '#ffc8aa'
                }.get(account_color_class, '#4D9DE0')
                
                html += f'<tr style="background-color: rgba(15, 124, 57, 0.3);">'
                for j, col in enumerate(cols):
                    css_class = ""
                    if col == 'Yesterday':
                        css_class = ' class="border-left"'
                    elif j > 0 and cols[j-1] == '30 Min':
                        css_class = ' class="border-left"'
                    
                    if col in ['Number', 'Website', 'Monetization', 'Account']:
                        if col == 'Account':
                            html += f'<td{css_class} style="color: {account_color};">{account}</td>'
                        else:
                            # Add no-border class for empty columns in summary rows
                            if css_class:
                                css_class = css_class.replace('"', ' no-border"')
                            else:
                                css_class = ' class="no-border"'
                            html += f'<td{css_class}></td>'
                    else:
                        # Sum numeric columns only for this account's websites
                        account_val = account_df[col].sum() if col in account_df.columns and not account_df.empty else 0
                        html += f'<td{css_class} style="color: white;">{account_val:,}</td>'
                html += '</tr>'
        
        # Add headers after total row with bold separator
        html += '<tr style="background-color: #262730; border-top: 4px solid #4D9DE0;">'
        for j, col in enumerate(cols):
            css_class = ""
            if col == 'Yesterday':
                css_class = ' class="border-left"'
            # Add border for first date column (after 30 Min)
            elif j > 0 and cols[j-1] == '30 Min':
                css_class = ' class="border-left"'
            html += f'<th{css_class}>{col}</th>'
        html += '</tr>'
        
        # Add individual website rows
        for i in range(len(df)):
            html += f'<tr>'
            for j, col in enumerate(cols):
                val = df.iloc[i, j]
                css_classes = []                # Add border classes
                if col == 'Yesterday':
                    css_classes.append('border-left')
                # Add border for first date column (after 30 Min)
                elif j > 0 and cols[j-1] == '30 Min':
                    css_classes.append('border-left')
                  # Add style classes
                if col == 'Website':
                    css_classes.append('website-name')
                elif col == 'Account':
                    css_classes.append('account-name')
                    # Add specific color class based on account name
                    if val and val.strip() and val in account_color_map:
                        css_classes.append(account_color_map[val])
                elif col == 'Monetization':
                    if val == 'ACTIVE':
                        css_classes.append('monetized-active')
                    elif val == 'REVIEW':
                        css_classes.append('monetized-review')
                    elif val == 'READY':
                        css_classes.append('monetized-ready')
                    else:
                        css_classes.append('monetized-none')
                        val = "NONE"  # Show "NONE" instead of blank
                
                class_attr = f' class="{" ".join(css_classes)}"' if css_classes else ''
                html += f'<td{class_attr}>{val}</td>'
            html += '</tr>'
        
        html += '</tbody></table></div>'
        return html

      # Generate and display HTML table
    html_table = create_unified_html_table(websites_performance_df, websites_performance_df)
    st.markdown(html_table, unsafe_allow_html=True)

    # All Selected Sites panel
    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)
    all_left_col, all_right_col = st.columns([0.7, 0.3])
    with all_left_col:
        st.markdown(
            f'<p class="site-title">Total</p>', unsafe_allow_html=True)
        all_daily = (
            filtered_df
            .groupby(filtered_df['event_date'].dt.date)['sessions']
            .sum()
            .reset_index()
            .sort_values('event_date')
        )
        daily_fig = px.line(all_daily, x='event_date', y='sessions')
        daily_fig.update_traces(hovertemplate='%{y:,}', line=dict(width=2))
        daily_fig.update_layout(
            height=340,
            template="plotly_dark",
            margin=dict(l=0, r=20, t=0, b=0),
            xaxis_title="",
            yaxis_title="",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(
                autorange=True,
                showgrid=True,
                nticks=10,
                gridcolor='rgba(255,255,255,0.1)',
                ticks="outside",
                showticklabels=True,
                minor=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
            )
        )
        st.plotly_chart(daily_fig, use_container_width=True,
                        config={'displayModeBar': False})

    with all_right_col:
        all_recent = filtered_df[filtered_df['minutes_past'] <= 30]
        all_by_minute = all_recent.groupby('minutes_past')['sessions'] \
            .sum() \
            .reindex(range(31), fill_value=0)
        all_active = int(all_recent['sessions'].sum())
        st.markdown(
            '<p class="compact-header">Active Users (Last 30 Minutes)</p>', unsafe_allow_html=True)
        st.markdown(
            f'<p class="compact-value">{all_active:,}</p>', unsafe_allow_html=True)

        bar_fig = go.Figure(go.Bar(
            x=all_by_minute.index,
            y=all_by_minute.values,
            marker_color='#4D9DE0',
            width=0.6,
            hovertemplate='%{y:,}<extra></extra>'
        ))
        bar_fig.update_layout(
            height=120,
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis=dict(tickmode='array', tickvals=[0, 10, 20, 30]),
            yaxis=dict(autorange=False, range=[
                all_by_minute.min(), all_by_minute.max()]),
            template="plotly_dark",
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(bar_fig, use_container_width=True,
                        config={'displayModeBar': False})
        # All Selected Sites - Top Countries (last 30 minutes only)
        all_country_df = (
            all_recent.groupby('country')['sessions']
            .sum()
            .nlargest(3)
            .reset_index()
        )
        if not all_country_df.empty:
            all_country_html = ""
            for _, row in all_country_df.iterrows():
                all_country_html += f"""
            <div class="country-line">
                <div class="country-name">{row['country']}</div>
                <div class="country-value">{int(row['sessions'])}</div>
            </div>
            """
            st.markdown(all_country_html, unsafe_allow_html=True)
        else:
            st.info("No country data available for selected sites")
    # All Selected Sites - Top 10 Landing Pages (Yesterday)
    st.markdown('<p class="site-title">Top 10 Landing Pages (Yesterday)</p>',
                unsafe_allow_html=True)
    if 'landingPage' in all_recent.columns:
        yesterday = pd.Timestamp.now().date() - pd.Timedelta(days=1)
        last_yesterday_df = filtered_df[filtered_df['event_date'].dt.date == yesterday]
        all_landing_page_table = (
            last_yesterday_df.groupby('landingPage', dropna=False)['sessions']
            .sum()
            .reset_index()
            .sort_values('sessions', ascending=False)
            .head(20)
        )
        all_landing_page_table = all_landing_page_table.rename(
            columns={'landingPage': 'Landing Page', 'sessions': 'Sessions'})
        st.dataframe(all_landing_page_table,
                     use_container_width=True, hide_index=True, height=180)
    else:
        st.info('No landing page data available for selected sites.')

    # Display dashboard for each selected website, ordered by total sessions (descending)
    website_order = (
        filtered_df.groupby('website')['sessions'].sum(
        ).sort_values(ascending=False).index.tolist()
    )
    for website in website_order:
        if website not in selected_websites:
            continue
        website_df = filtered_df[filtered_df['website'] == website]

        recent = website_df[website_df['minutes_past'] <= 30]
        active_users = int(recent['sessions'].sum())

        st.markdown(
            "<hr style='margin:0; padding:0; height:1px; border:none; background-color:#e0e0e0;'>", unsafe_allow_html=True)

        left_col, right_col = st.columns([0.7, 0.3])
        with left_col:
            st.markdown(
                f'<p class="site-title">{website}</p>', unsafe_allow_html=True)
            if 'event_date' in website_df.columns and not website_df.empty:
                daily_users = website_df.groupby(website_df['event_date'].dt.date)[
                    'sessions'].sum().reset_index().sort_values('event_date')
                if not daily_users.empty:
                    daily_fig = px.line(
                        daily_users, x='event_date', y='sessions')
                    daily_fig.update_traces(
                        hovertemplate='%{y:,}', line=dict(width=2))
                    daily_fig.update_layout(
                        height=340,
                        template="plotly_dark",
                        margin=dict(l=0, r=20, t=0, b=0),
                        xaxis_title="",
                        yaxis_title="",
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        yaxis=dict(
                            autorange=True,
                            showgrid=True,
                            nticks=10,
                            gridcolor='rgba(255,255,255,0.1)',
                            ticks="outside",
                            showticklabels=True,
                            minor=dict(
                                showgrid=True,
                                gridcolor='rgba(255,255,255,0.05)'
                            )
                        )
                    )
                    st.plotly_chart(daily_fig, use_container_width=True, config={
                                    'displayModeBar': False})

        with right_col:
            st.markdown(
                '<p class="compact-header">Active Users (Last 30 Minutes)</p>', unsafe_allow_html=True)
            st.markdown(
                f'<p class="compact-value">{active_users:,}</p>', unsafe_allow_html=True)

            if not (recent['sessions'] == 0).all():
                pm = recent.groupby('minutes_past')[
                    'sessions'].sum().reindex(range(31), fill_value=0)
                y_min_bar = pm.min()
                y_max_bar = pm.max()
                bar_fig = go.Figure(go.Bar(
                    x=pm.index,
                    y=pm.values,
                    marker_color='#4D9DE0',
                    width=0.6,
                    hovertemplate='%{y:,}<extra></extra>'
                ))
                bar_fig.update_layout(
                    height=120,
                    margin=dict(l=0, r=0, t=0, b=0),
                    xaxis=dict(tickmode='array', tickvals=[0, 10, 20, 30]),
                    yaxis=dict(range=[y_min_bar, y_max_bar], autorange=False),
                    template="plotly_dark",
                    showlegend=False,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(bar_fig, use_container_width=True,
                                config={'displayModeBar': False})

                country_df = recent.groupby(
                    'country')['sessions'].sum().nlargest(3).reset_index()

                if not country_df.empty:
                    country_html = ""
                for _, row in country_df.iterrows():
                    country_html += f"""
                    <div class="country-line">
                        <div class="country-name">{row['country']}</div>
                        <div class="country-value">{int(row['sessions'])}</div>
                    </div>
                    """
                st.markdown(country_html, unsafe_allow_html=True)
        # Landing Page Analysis Table (Top 10 by sessions, yesterday)
        st.markdown(
            '<p class="site-title">Top 10 Landing Pages (Yesterday)</p>', unsafe_allow_html=True)
        if 'landingPage' in website_df.columns:
            yesterday = pd.Timestamp.now().date() - pd.Timedelta(days=1)
            last_yesterday_df = website_df[website_df['event_date'].dt.date == yesterday]
            landing_page_table = (
                last_yesterday_df.groupby(
                    'landingPage', dropna=False)['sessions']
                .sum()
                .reset_index()
                .sort_values('sessions', ascending=False)
                .head(10)
            )
            landing_page_table = landing_page_table.rename(
                columns={'landingPage': 'Landing Page', 'sessions': 'Sessions'})
            st.dataframe(landing_page_table, use_container_width=True,
                         hide_index=True, height=180)  # ~5 rows visible, scroll for more
        else:
            st.info('No landing page data available for this website.')
        # --- Top 3 Sources for Yesterday (moved to end) ---
        yesterday = pd.Timestamp.now().date() - pd.Timedelta(days=1)
        yesterday_website_df = website_df[website_df['event_date'].dt.date == yesterday]
        if 'session_source' in yesterday_website_df.columns:
            top_sources = (
                yesterday_website_df.groupby('session_source')['sessions']
                .sum()
                .reset_index()
                .sort_values('sessions', ascending=False)
                .head(3)
            )
            if not top_sources.empty:
                cols = st.columns(3)
                for i, (_, row) in enumerate(top_sources.iterrows()):
                    with cols[i]:
                        st.markdown(
                            f"<div style='text-align:center;'><b>{row['session_source'] if row['session_source'] else '(not set)'}</b><br><span style='font-size:22px'>{int(row['sessions'])}</span></div>", unsafe_allow_html=True)
            else:
                st.info('No source data available for yesterday.')
        else:
            st.info('No source data available for this website.')
        # --- Top 3 Sources Trend (Last 7 Days) ---
        last_7_days = pd.Timestamp.now().date() - pd.Timedelta(days=6)
        last7_df = website_df[website_df['event_date'].dt.date >= last_7_days]
        if 'session_source' in last7_df.columns:
            # Find top 3 sources over the last 7 days
            top3_sources = (
                last7_df.groupby('session_source')['sessions']
                .sum()
                .sort_values(ascending=False)
                .head(3)
                .index.tolist()
            )
            trend_df = last7_df[last7_df['session_source'].isin(top3_sources)]
            if not trend_df.empty:
                trend_chart = px.line(
                    trend_df.groupby(['event_date', 'session_source'])[
                        'sessions'].sum().reset_index(),
                    x='event_date', y='sessions', color='session_source',
                    title='Top 3 Traffic Sources (Last 7 Days)',
                    labels={'sessions': 'Sessions',
                            'event_date': 'Date', 'session_source': 'Source'}
                )
                trend_chart.update_traces(mode='lines+markers')
                trend_chart.update_layout(
                    height=260,
                    template="plotly_dark",
                    margin=dict(l=0, r=0, t=30, b=0),
                    legend_title_text='Source',
                    xaxis_title='',
                    yaxis_title='Sessions',
                )
                st.plotly_chart(trend_chart, use_container_width=True, config={
                                'displayModeBar': False})
            else:
                st.info('No source trend data available for the last 7 days.')
        else:
            st.info('No source data available for this website.')
        st.markdown(
            "<hr style='margin:0; padding:0; height:1px; border:none; background-color:#e0e0e0;'>", unsafe_allow_html=True)
    import altair as alt
    st.markdown('---')
    st.header('Usage & Report Cost Monitor')
    if cost_df is not None and not cost_df.empty:
        # Ensure date is just the date (no time)
        cost_df['date'] = pd.to_datetime(cost_df['date']).dt.date
        # Generate last 30 days date range
        last_30_days = pd.date_range(end=pd.Timestamp.now().date(), periods=30)
        last_30_days_df = pd.DataFrame({'date': last_30_days.date})
        # Aggregate by day (if not already)
        daily_cost = cost_df.groupby('date', as_index=False).agg({
            'gigs_billed': 'sum',
            'month_to_date_gigs_billed_sum': 'last'
        })
        # Merge with full date range to ensure all days are present
        daily_cost = last_30_days_df.merge(daily_cost, on='date', how='left').fillna(
            {'gigs_billed': 0, 'month_to_date_gigs_billed_sum': 0})
        # Metrics
        total_gb = daily_cost['gigs_billed'].sum()
        percent_free = (total_gb / 1024) * 100
        metric_col1, metric_col2 = st.columns([0.5, 0.5])

        with metric_col1:
            st.markdown(
                "<h3 style='text-align: center'>Gigs Billed (last 30 days)</h3>", unsafe_allow_html=True)
            st.markdown(
                f"<h1 style='text-align: center;'>{total_gb:.2f} GiB</h1>", unsafe_allow_html=True)

        with metric_col2:
            st.markdown(
                "<h3 style='text-align: center'>% of Free Tier Used (1 TiB)</h3>", unsafe_allow_html=True)
            st.markdown(
                f"<h1 style='text-align: center;'>{percent_free:.2f}%</h1>", unsafe_allow_html=True)
        # Chart
        base = alt.Chart(daily_cost).encode(
            x=alt.X('date:T', title='Date', axis=alt.Axis(format='%b %d'))
        )
        bar = base.mark_bar(color='#21807a').encode(
            y=alt.Y('gigs_billed:Q', title='Gigs Billed'),
            tooltip=['date:T', 'gigs_billed:Q']
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info('No BigQuery usage data available.')

    # Website filter and refresh button at the bottom
    st.markdown('---')
    st.subheader('Filter Websites')
    filter_col, button_col = st.columns([4, 1])

    with filter_col:
        # Allow user to select multiple websites (all selected by default)
        st.multiselect(
            "Select websites to display above:", websites, default=websites, key="website_filter")

    with button_col:
        # Add refresh button
        if st.button("🔄 Refresh Data", use_container_width=True, help="Click to refresh data from BigQuery", key="refresh_bottom"):
            st.session_state.refresh_clicked = True
            st.rerun()

    return True
