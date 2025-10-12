import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import requests
import asyncio
import websockets
import time
import datetime
from datetime import datetime as dt
import numpy as np

# Set page configuration
st.set_page_config(
    page_title="Real-time Vessel Status Monitoring",
    page_icon="🚢",
    layout="wide"
)

st.title("Real-time Vessel Status Monitoring System 🚢")
st.markdown("""
**Real-time Vessel Position and Speed Monitoring System** - Using streaming data technology to display global vessel real-time status
""")

# Initialize session state
if 'vessel_data' not in st.session_state:
    st.session_state.vessel_data = pd.DataFrame()
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'stats' not in st.session_state:
    st.session_state.stats = {}

# Sidebar controls
st.sidebar.header("Control Panel")
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 30, 10)
use_websocket = st.sidebar.checkbox("Use WebSocket real-time stream", True)
show_stationary = st.sidebar.checkbox("Show stationary vessels", True)

# Select data source
data_source = st.sidebar.selectbox(
    "Select data source",
    ["Simulated streaming data", "API interface"]
)

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

# Speed distribution chart
st.subheader("Speed Distribution")
speed_dist_plot = st.empty()

# Vessel type distribution
st.subheader("Vessel Type Distribution")
type_dist_plot = st.empty()

# Real-time data update function
def fetch_vessel_data():
    """Get vessel data"""
    if data_source == "API interface":
        try:
            response = requests.get("http://localhost:8000/api/vessel-data")
            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data['vessels']), data.get('stats', {})
            else:
                st.error(f"API request failed, status code: {response.status_code}")
                return pd.DataFrame(), {}
        except Exception as e:
            st.error(f"Failed to get API data: {str(e)}")
            return pd.DataFrame(), {}
    else:
        # Use simulated data
        # Note: In a real application, this would connect to the FastAPI WebSocket endpoint
        return pd.DataFrame(), {}

def update_dashboard():
    """Update dashboard"""
    if data_source == "Simulated streaming data":
        # Create simulated data for demonstration
        n_vessels = 50
        data = {
            'MMSI': [f"123456{i:03d}" for i in range(n_vessels)],
            'NAME': [f"Vessel_{i}" for i in range(n_vessels)],
            'LATITUDE': np.random.uniform(-90, 90, n_vessels),
            'LONGITUDE': np.random.uniform(-180, 180, n_vessels),
            'SOG': np.random.uniform(0, 25, n_vessels),  # Speed over ground
            'COG': np.random.uniform(0, 360, n_vessels),  # Course over ground
            'TYPE': np.random.choice(['Cargo', 'Tanker', 'Passenger', 'Fishing', 'Other'], n_vessels),
            'TIMESTAMP': [datetime.datetime.now()] * n_vessels
        }
        df = pd.DataFrame(data)
        
        # Randomly set some vessels as stationary (SOG < 0.5)
        stationary_mask = np.random.rand(n_vessels) < 0.3
        df.loc[stationary_mask, 'SOG'] = 0
        
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
    else:
        df, stats = fetch_vessel_data()
        if not df.empty:
            st.session_state.vessel_data = df
            st.session_state.last_update = datetime.datetime.now()
            st.session_state.stats = stats

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
    
    # Create scatter map
    fig = px.scatter_mapbox(
        df, 
        lat="LATITUDE", 
        lon="LONGITUDE", 
        color="SOG",
        size="SOG",
        hover_name="NAME",
        hover_data=["MMSI", "SOG", "COG", "TYPE"],
        color_continuous_scale=px.colors.sequential.Viridis,
        size_max=15,
        zoom=1,
        height=600
    )
    
    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )
    
    map_plot.plotly_chart(fig, use_container_width=True)

def render_stats():
    """Render statistics metrics"""
    stats = st.session_state.stats
    
    if stats:
        total_vessels.metric("Total vessels", f"{stats.get('total_vessels', 0)}")
        active_vessels.metric("Active vessels", f"{stats.get('active_vessels', 0)}")
        avg_speed.metric("Average speed (knots)", f"{stats.get('avg_sog', 0):.2f}")
        max_speed.metric("Maximum speed (knots)", f"{stats.get('max_sog', 0):.2f}")
        stationary_vessels.metric("Stationary vessels", f"{stats.get('stationary_vessels', 0)}")
        data_points.metric("Data points", len(st.session_state.vessel_data))

def render_speed_distribution():
    """Render speed distribution chart"""
    df = st.session_state.vessel_data
    if df.empty:
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

# Initialize data
if st.session_state.vessel_data.empty:
    update_dashboard()

# Render components
render_map()
render_stats()
render_speed_distribution()
render_type_distribution()

# Auto-refresh mechanism
if use_websocket:
    # In actual application, this would connect to WebSocket
    st.sidebar.info("Connecting to WebSocket data stream...")
else:
    # Manual refresh button
    if st.sidebar.button("Manual refresh data"):
        update_dashboard()
        render_map()
        render_stats()
        render_speed_distribution()
        render_type_distribution()

# Auto update timer
st_autorefresh = None
try:
    from streamlit_autorefresh import st_autorefresh
    # Refresh page at specified intervals
    st_autorefresh(interval=refresh_interval * 1000, key="dashboard_refresh")
except ImportError:
    st.sidebar.warning("Please install streamlit-autorefresh to enable auto-refresh: pip install streamlit-autorefresh")

# Display last update time
if st.session_state.last_update:
    st.sidebar.caption(f"Last update: {st.session_state.last_update.strftime('%H:%M:%S')}")