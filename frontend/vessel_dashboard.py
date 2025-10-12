import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import asyncio
import websockets
import time
import datetime
from datetime import datetime as dt
import numpy as np
import random
from collections import deque
import altair as alt

# Set page configuration
st.set_page_config(
    page_title="Real-time Vessel Status Monitoring",
    page_icon="🚢",
    layout="wide"
)

st.title("Real-time Vessel Status Monitoring System 🚢")
st.markdown("""
**Real-time Vessel Position and Speed Monitoring System** - Using streaming data technologies to display global vessel real-time status
""")

# Initialize session state
if 'vessel_data' not in st.session_state:
    st.session_state.vessel_data = pd.DataFrame()
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'stats' not in st.session_state:
    st.session_state.stats = {}
if 'speed_history' not in st.session_state:
    st.session_state.speed_history = deque(maxlen=100)  # Keep last 100 speed values
if 'position_history' not in st.session_state:
    st.session_state.position_history = deque(maxlen=50)  # Keep last 50 positions

# Sidebar controls
st.sidebar.header("Control Panel")
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 30, 10)
use_simulated = st.sidebar.checkbox("Use simulated data", True)
show_stationary = st.sidebar.checkbox("Show stationary vessels", True)

# Sliding window settings
window_size = st.sidebar.slider("Sliding window size (minutes)", 1, 30, 5)

# Display current time
current_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
st.sidebar.markdown(f"**Current time:** {current_time}")

# Main page layout
col1, col2 = st.columns([2, 1])

with col1:
    # Map display area
    map_container = st.container()
    with map_container:
        st.subheader("Vessel Real-time Position Map")
        map_plot = st.empty()

with col2:
    # Statistics metrics area
    st.subheader("Real-time Statistics Metrics")
    stats_container = st.container()
    
    with stats_container:
        col2_1, col2_2 = st.columns(2)
        with col2_1:
            total_vessels = st.empty()
            active_vessels = st.empty()
            avg_speed = st.empty()
        with col2_2:
            max_speed = st.empty()
            stationary_vessels = st.empty()
            data_points = st.empty()

# Speed trend chart
st.subheader("Real-time Speed Trend")
speed_trend_plot = st.empty()

# Speed distribution chart
st.subheader("Speed Distribution")
speed_dist_plot = st.empty()

# Vessel type distribution
st.subheader("Vessel Type Distribution")
type_dist_plot = st.empty()

# Real-time data update function
async def fetch_websocket_data():
    """Get real-time vessel data via WebSocket"""
    try:
        async with websockets.connect("ws://localhost:8000/ws/vessel-data") as websocket:
            # Receive data
            data = await websocket.recv()
            json_data = json.loads(data)
            vessels = json_data.get('vessels', [])
            stats = json_data.get('stats', {})
            
            # Convert to DataFrame
            df = pd.DataFrame(vessels)
            
            # Update session state
            st.session_state.vessel_data = df
            st.session_state.last_update = dt.fromisoformat(json_data['timestamp'].replace('Z', '+00:00'))
            st.session_state.stats = stats
            
            # Update historical data for sliding window analysis
            if not df.empty and 'SOG' in df.columns:
                for speed in df['SOG'].dropna():
                    st.session_state.speed_history.append(speed)
                
                positions = df[['LATITUDE', 'LONGITUDE']].dropna()
                for _, row in positions.iterrows():
                    st.session_state.position_history.append((row['LATITUDE'], row['LONGITUDE']))
            
            return df, stats
    except Exception as e:
        st.error(f"WebSocket connection failed: {str(e)}")
        return pd.DataFrame(), {}

def generate_simulated_data():
    """Generate simulated vessel data"""
    n_vessels = random.randint(15, 35)
    
    # Generate random vessel data
    data = {
        'MMSI': [f"123456{i:03d}" for i in range(n_vessels)],
        'NAME': [f"Vessel_{i}" for i in range(n_vessels)],
        'LATITUDE': np.random.uniform(-60, 80, n_vessels),  # Avoid polar regions
        'LONGITUDE': np.random.uniform(-180, 180, n_vessels),
        'SOG': np.random.exponential(5, n_vessels),  # Speed over ground, using exponential distribution for more realism
        'COG': np.random.uniform(0, 360, n_vessels),  # Course over ground
        'TYPE': np.random.choice([
            'Cargo', 'Tanker', 'Passenger', 'Fishing', 'Tug', 'Pilot', 'Sailing', 'Pleasure'
        ], n_vessels, p=[0.3, 0.2, 0.1, 0.15, 0.1, 0.05, 0.05, 0.05]),
        'TIMESTAMP': [datetime.datetime.now()] * n_vessels
    }
    
    df = pd.DataFrame(data)
    
    # Limit speed to reasonable range
    df['SOG'] = np.clip(df['SOG'], 0, 30)
    
    # Randomly set some vessels as stationary
    stationary_count = random.randint(3, 8)
    stationary_indices = random.sample(range(n_vessels), stationary_count)
    df.loc[stationary_indices, 'SOG'] = 0.0
    
    # Update session state
    st.session_state.vessel_data = df
    st.session_state.last_update = datetime.datetime.now()
    
    # Calculate statistics
    active_mask = df['SOG'] > 0.5
    stats = {
        'total_vessels': len(df),
        'active_vessels': active_mask.sum(),
        'stationary_vessels': (~active_mask).sum(),
        'avg_sog': df['SOG'].mean(),
        'max_sog': df['SOG'].max(),
        'vessel_types': df['TYPE'].value_counts().to_dict()
    }
    st.session_state.stats = stats
    
    # Update historical data
    for speed in df['SOG']:
        st.session_state.speed_history.append(speed)
    
    for _, row in df[['LATITUDE', 'LONGITUDE']].iterrows():
        st.session_state.position_history.append((row['LATITUDE'], row['LONGITUDE']))
    
    return df, stats

def update_dashboard():
    """Update dashboard"""
    if use_simulated:
        df, stats = generate_simulated_data()
    else:
        try:
            # Since Streamlit cannot run async functions directly, use HTTP API as alternative
            import requests
            response = requests.get("http://localhost:8000/api/vessel-data")
            if response.status_code == 200:
                data = response.json()
                vessels = data.get('vessels', [])
                stats = data.get('stats', {})
                
                df = pd.DataFrame(vessels)
                
                # Update session state
                st.session_state.vessel_data = df
                st.session_state.last_update = dt.fromisoformat(data['timestamp'].replace('Z', '+00:00')) if 'timestamp' in data else datetime.datetime.now()
                st.session_state.stats = stats
                
                # Update historical data for sliding window analysis
                if not df.empty and 'SOG' in df.columns:
                    for speed in df['SOG'].dropna():
                        st.session_state.speed_history.append(speed)
                    
                    positions = df[['LATITUDE', 'LONGITUDE']].dropna()
                    for _, row in positions.iterrows():
                        st.session_state.position_history.append((row['LATITUDE'], row['LONGITUDE']))
                
                return df, stats
            else:
                st.error(f"API request failed, status code: {response.status_code}")
                df, stats = generate_simulated_data()
        except Exception as e:
            st.error(f"Failed to connect to API: {str(e)}")
            df, stats = generate_simulated_data()

def render_map():
    """Render vessel position map"""
    df = st.session_state.vessel_data
    if df.empty:
        st.warning("No vessel data available")
        return
    
    # Filter stationary vessels (if not selected to show)
    if not show_stationary:
        df = df[df['SOG'] > 0.5]
    
    if df.empty:
        st.warning("No vessel data matching the criteria")
        return
    
    # Add color coding for map
    df['speed_category'] = pd.cut(df['SOG'], 
                                  bins=[0, 1, 5, 10, 20, 30], 
                                  labels=['Stationary(0)', 'Slow(1-5)', 'Moderate(5-10)', 'Fast(10-20)', 'High(20+)'])
    
    # Create scatter map
    fig = px.scatter_mapbox(
        df, 
        lat="LATITUDE", 
        lon="LONGITUDE", 
        color="speed_category",
        size="SOG",
        hover_name="NAME",
        hover_data=["MMSI", "SOG", "COG", "TYPE"],
        color_discrete_map={
            'Stationary(0)': 'red',
            'Slow(1-5)': 'orange', 
            'Moderate(5-10)': 'yellow',
            'Fast(10-20)': 'lightblue',
            'High(20+)': 'darkblue'
        },
        size_max=15,
        zoom=1,
        height=600,
        title="Global Vessel Real-time Positions"
    )
    
    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 30, "l": 0, "b": 0}
    )
    
    map_plot.plotly_chart(fig, use_container_width=True)

def render_stats():
    """Render statistics metrics"""
    stats = st.session_state.stats
    
    if stats:
        total_vessels.metric("Total Vessels", f"{stats.get('total_vessels', 0)}")
        active_vessels.metric("Active Vessels", f"{stats.get('active_vessels', 0)}")
        avg_speed.metric("Average Speed (knots)", f"{stats.get('avg_sog', 0):.2f}")
        max_speed.metric("Maximum Speed (knots)", f"{stats.get('max_sog', 0):.2f}")
        stationary_vessels.metric("Stationary Vessels", f"{stats.get('stationary_vessels', 0)}")
        if st.session_state.vessel_data is not None:
            data_points.metric("Current Data Points", len(st.session_state.vessel_data))

def render_speed_trend():
    """Render speed trend chart"""
    if len(st.session_state.speed_history) > 0:
        # Create data frame with time series
        times = list(range(len(st.session_state.speed_history)))
        speeds = list(st.session_state.speed_history)
        
        trend_df = pd.DataFrame({'time': times, 'speed': speeds})
        
        # Create chart
        chart = alt.Chart(trend_df).mark_line().encode(
            x=alt.X('time:O', title='Time Point'),
            y=alt.Y('speed:Q', title='Speed (knots)', scale=alt.Scale(domain=[0, max(speeds + [1]) if speeds else 1])),
            tooltip=['time', 'speed']
        ).properties(
            title='Real-time Speed Trend',
            height=300
        )
        
        speed_trend_plot.altair_chart(chart, use_container_width=True)

def render_speed_distribution():
    """Render speed distribution chart"""
    df = st.session_state.vessel_data
    if df.empty or 'SOG' not in df.columns:
        return
    
    fig = px.histogram(
        df, 
        x="SOG", 
        nbins=30,
        title="Speed Distribution Histogram",
        labels={'SOG': 'Speed (knots)', 'count': 'Vessel Count'}
    )
    fig.update_layout(showlegend=False)
    speed_dist_plot.plotly_chart(fig, use_container_width=True)

def render_type_distribution():
    """Render vessel type distribution chart"""
    stats = st.session_state.stats
    if stats and 'vessel_types' in stats:
        type_counts = stats['vessel_types']
        if type_counts:
            df_types = pd.DataFrame(list(type_counts.items()), columns=['Type', 'Count'])
            
            fig = px.pie(
                df_types, 
                values='Count', 
                names='Type',
                title="Vessel Type Distribution"
            )
            type_dist_plot.plotly_chart(fig, use_container_width=True)

def render_sliding_window_stats():
    """Render sliding window statistics"""
    if len(st.session_state.speed_history) > 0:
        st.subheader(f"Sliding Window Statistics (Last {window_size} minutes)")
        
        # Calculate sliding window statistics
        recent_speeds = list(st.session_state.speed_history)[-int(window_size*6):]  # Assuming 10-second data points
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Window Average Speed", f"{np.mean(recent_speeds):.2f}" if recent_speeds else "0.00")
        with col2:
            st.metric("Window Maximum Speed", f"{max(recent_speeds):.2f}" if recent_speeds else "0.00")
        with col3:
            st.metric("Window Vessel Count", len(recent_speeds))

# Initialize data
if st.session_state.vessel_data.empty or st.session_state.last_update is None:
    update_dashboard()

# Render components
render_map()
render_stats()
render_speed_trend()
render_speed_distribution()
render_type_distribution()
render_sliding_window_stats()

# Auto-refresh mechanism
import os
os.environ["STREAMLIT_SERVER_ENABLE_STATIC_SERVE"] = "true"
    
# Manual refresh button
if st.sidebar.button("Manual Refresh Data"):
    update_dashboard()
    st.rerun()

# Display last update time
if st.session_state.last_update:
    st.sidebar.caption(f"Last update: {st.session_state.last_update.strftime('%H:%M:%S')}")

# Display system information
st.sidebar.subheader("System Information")
st.sidebar.info(f"""
- Data Source: {"Simulated Data" if use_simulated else "WebSocket API"}
- Refresh Interval: {refresh_interval} seconds
- Sliding Window: {window_size} minutes
- Current Vessels: {len(st.session_state.vessel_data)} vessels
""")