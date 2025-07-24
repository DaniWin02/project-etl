import os
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-webserver:8080/api/v1")
AIRFLOW_UI_URL = os.getenv("AIRFLOW_UI_URL", "http://airflow-webserver:8080")

airflow_user = os.getenv("AIRFLOW_USER", "admin")
airflow_pass = os.getenv("AIRFLOW_PASSWORD", "admin")

# Page config with custom styling
st.set_page_config(
    page_title="ETL Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with dark theme
st.markdown("""
<style>
    /* Main app background */
    .stApp {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
    }
    
    .main-header {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    
    .metric-card {
        background: rgba(255,255,255,0.95);
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.2);
        border-left: 4px solid #ff6b6b;
    }
    
    .status-success {
        color: #10ac84;
        font-weight: bold;
    }
    
    .status-error {
        color: #ee5a24;
        font-weight: bold;
    }
    
    .status-warning {
        color: #feca57;
        font-weight: bold;
    }
    
    .sidebar-section {
        background: rgba(255,255,255,0.1);
        backdrop-filter: blur(10px);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border: 1px solid rgba(255,255,255,0.2);
    }
    
    .data-section {
        background: rgba(255,255,255,0.95);
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
        margin-bottom: 1rem;
        backdrop-filter: blur(10px);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: rgba(30,60,114,0.8);
        backdrop-filter: blur(10px);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: rgba(255,255,255,0.1);
        border-radius: 8px;
        color: white;
        font-weight: bold;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
    }
</style>
""", unsafe_allow_html=True)

def test_airflow_connection():
    try:
        url = f"{AIRFLOW_API_BASE}/version"
        response = requests.get(url, auth=HTTPBasicAuth(airflow_user, airflow_pass), timeout=10)
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        return None, str(e)

def get_database_stats():
    """Get statistics from all databases"""
    stats = {}
    try:
        # Weather stats
        weather_db = client["weather_db"]
        weather_col = weather_db["forecast"]
        stats['weather'] = {
            'count': weather_col.count_documents({}),
            'last_update': weather_col.find_one(sort=[("fetched_at", -1)])
        }
        
        # Universities stats
        uni_db = client["universities_db"]
        uni_col = uni_db["universities"]
        stats['universities'] = {
            'count': uni_col.count_documents({}),
            'last_update': uni_col.find_one(sort=[("fetched_at", -1)])
        }
        
        # Books stats
        books_db = client["books_db"]
        books_col = books_db["books"]
        stats['books'] = {
            'count': books_col.count_documents({}),
            'last_update': books_col.find_one(sort=[("fetched_at", -1)])
        }
        
    except Exception as e:
        st.error(f"Error getting database stats: {e}")
    
    return stats

def fetch_data(db_name, col_name, limit):
    try:
        db = client[db_name]
        col = db[col_name]
        return list(col.find().sort("fetched_at", -1).limit(limit))
    except Exception as e:
        st.error(f"Error fetching data from {db_name}.{col_name}: {e}")
        return []

def get_dag_runs(dag_id, limit=5):
    try:
        url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
        params = {'order_by': '-execution_date', 'limit': limit}
        response = requests.get(url, params=params, auth=HTTPBasicAuth(airflow_user, airflow_pass), timeout=30)
        if response.status_code == 200:
            return response.json().get("dag_runs", [])
        else:
            st.error(f"Error fetching DAG runs: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error connecting to Airflow: {e}")
        return []

def create_weather_charts(df_weather):
    """Create enhanced weather visualizations with meaningful insights"""
    try:
        if len(df_weather) > 0:
            # Convert fetched_at to datetime
            if "fetched_at" in df_weather.columns:
                df_weather["fetched_at"] = pd.to_datetime(df_weather["fetched_at"], errors='coerce')
                df_weather = df_weather.dropna(subset=['fetched_at']).sort_values("fetched_at")
            
            # Chart 1: Temperature trends over time
            if "temperature" in df_weather.columns and len(df_weather) > 1:
                fig_temp = px.line(
                    df_weather, 
                    x="fetched_at", 
                    y="temperature",
                    title="🌡️ Tendencia de Temperatura",
                    labels={"temperature": "Temperatura (°C)", "fetched_at": "Tiempo"},
                    markers=True,
                    color_discrete_sequence=['#ff6b6b']
                )
                fig_temp.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(size=12, color='#2c3e50'),
                    title_font=dict(size=16, color='#2c3e50'),
                    showlegend=False
                )
                fig_temp.update_traces(line=dict(width=3), marker=dict(size=6))
                st.plotly_chart(fig_temp, use_container_width=True)
            
            # Chart 2: Temperature distribution (histogram)
            if "temperature" in df_weather.columns and len(df_weather) > 5:
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_hist = px.histogram(
                        df_weather,
                        x="temperature",
                        title="📊 Distribución de Temperaturas",
                        labels={"temperature": "Temperatura (°C)", "count": "Frecuencia"},
                        nbins=10,
                        color_discrete_sequence=['#ff6b6b']
                    )
                    fig_hist.update_layout(
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(size=10, color='#2c3e50'),
                        title_font=dict(size=14, color='#2c3e50'),
                        showlegend=False
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)
                
                # Chart 3: Temperature vs Humidity correlation (if humidity exists)
                with col2:
                    if "humidity" in df_weather.columns:
                        fig_scatter = px.scatter(
                            df_weather,
                            x="temperature",
                            y="humidity",
                            title="🌡️💧 Temperatura vs Humedad",
                            labels={"temperature": "Temperatura (°C)", "humidity": "Humedad (%)"},
                            color="temperature",
                            color_continuous_scale="Reds",
                            size_max=10
                        )
                        fig_scatter.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(size=10, color='#2c3e50'),
                            title_font=dict(size=14, color='#2c3e50')
                        )
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    else:
                        # Weather conditions pie chart if available
                        if "condition" in df_weather.columns:
                            condition_counts = df_weather["condition"].value_counts()
                            fig_pie = px.pie(
                                values=condition_counts.values,
                                names=condition_counts.index,
                                title="☁️ Condiciones Climáticas",
                                color_discrete_sequence=px.colors.qualitative.Set3
                            )
                            fig_pie.update_layout(
                                plot_bgcolor="rgba(0,0,0,0)",
                                paper_bgcolor="rgba(0,0,0,0)",
                                font=dict(size=10, color='#2c3e50'),
                                title_font=dict(size=14, color='#2c3e50')
                            )
                            st.plotly_chart(fig_pie, use_container_width=True)
                        
    except Exception as e:
        st.error(f"Error creating weather charts: {e}")

def create_universities_charts(df_uni):
    """Create meaningful university analytics"""
    try:
        if not df_uni.empty:
            col1, col2 = st.columns(2)
            
            # Chart 1: Universities by country (top 15)
            with col1:
                if "country" in df_uni.columns:
                    country_counts = df_uni["country"].value_counts().head(15)
                    if len(country_counts) > 0:
                        fig_countries = px.bar(
                            y=country_counts.index,
                            x=country_counts.values,
                            orientation='h',
                            title="🏛️ Universidades por País (Top 15)",
                            labels={"x": "Número de Universidades", "y": "País"},
                            color=country_counts.values,
                            color_continuous_scale='Reds'
                        )
                        fig_countries.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(size=10, color='#2c3e50'),
                            title_font=dict(size=14, color='#2c3e50'),
                            height=400
                        )
                        st.plotly_chart(fig_countries, use_container_width=True)
            
            # Chart 2: Domain analysis or regional distribution
            with col2:
                if "domains" in df_uni.columns:
                    # Extract domain extensions for analysis
                    domains_data = []
                    for domains in df_uni["domains"].dropna():
                        if isinstance(domains, list) and len(domains) > 0:
                            domain = domains[0] if domains[0] else ""
                            if '.' in domain:
                                extension = domain.split('.')[-1].upper()
                                domains_data.append(extension)
                    
                    if domains_data:
                        domain_counts = pd.Series(domains_data).value_counts().head(10)
                        fig_domains = px.pie(
                            values=domain_counts.values,
                            names=domain_counts.index,
                            title="🌐 Dominios más Comunes",
                            color_discrete_sequence=px.colors.qualitative.Pastel
                        )
                        fig_domains.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(size=10, color='#2c3e50'),
                            title_font=dict(size=14, color='#2c3e50')
                        )
                        st.plotly_chart(fig_domains, use_container_width=True)
                else:
                    # Alternative: Show name length distribution
                    if "name" in df_uni.columns:
                        df_uni['name_length'] = df_uni['name'].str.len()
                        fig_length = px.histogram(
                            df_uni,
                            x="name_length",
                            title="📏 Distribución por Longitud del Nombre",
                            labels={"name_length": "Caracteres en el Nombre", "count": "Frecuencia"},
                            nbins=20,
                            color_discrete_sequence=['#74b9ff']
                        )
                        fig_length.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(size=10, color='#2c3e50'),
                            title_font=dict(size=14, color='#2c3e50')
                        )
                        st.plotly_chart(fig_length, use_container_width=True)
                        
    except Exception as e:
        st.error(f"Error creating university charts: {e}")

def create_books_charts(df_books):
    """Create comprehensive book analytics"""
    try:
        if not df_books.empty:
            # Chart 1: Publication timeline
            if "first_publish_year" in df_books.columns:
                df_books_clean = df_books.dropna(subset=['first_publish_year'])
                df_books_clean = df_books_clean[
                    (df_books_clean["first_publish_year"] >= 1800) & 
                    (df_books_clean["first_publish_year"] <= 2024)
                ]
                
                if len(df_books_clean) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Decade-based grouping for better visualization
                        df_books_clean['decade'] = (df_books_clean['first_publish_year'] // 10) * 10
                        decade_counts = df_books_clean['decade'].value_counts().sort_index()
                        
                        fig_decades = px.bar(
                            x=decade_counts.index,
                            y=decade_counts.values,
                            title="📚 Publicaciones por Década",
                            labels={"x": "Década", "y": "Número de Libros"},
                            color=decade_counts.values,
                            color_continuous_scale='Purples'
                        )
                        fig_decades.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(size=10, color='#2c3e50'),
                            title_font=dict(size=14, color='#2c3e50')
                        )
                        st.plotly_chart(fig_decades, use_container_width=True)
                    
                    with col2:
                        # Author productivity analysis
                        if "author" in df_books.columns:
                            author_counts = df_books["author"].value_counts().head(10)
                            if len(author_counts) > 0:
                                fig_authors = px.bar(
                                    y=author_counts.index,
                                    x=author_counts.values,
                                    orientation='h',
                                    title="✍️ Autores más Prolíficos (Top 10)",
                                    labels={"x": "Número de Libros", "y": "Autor"},
                                    color=author_counts.values,
                                    color_continuous_scale='Viridis'
                                )
                                fig_authors.update_layout(
                                    plot_bgcolor="rgba(0,0,0,0)",
                                    paper_bgcolor="rgba(0,0,0,0)",
                                    font=dict(size=10, color='#2c3e50'),
                                    title_font=dict(size=14, color='#2c3e50'),
                                    height=350
                                )
                                st.plotly_chart(fig_authors, use_container_width=True)
            
            # Chart 3: Subject/Genre analysis if available
            if "subject" in df_books.columns:
                # Extract subjects and create word cloud-style analysis
                subjects_list = []
                for subjects in df_books["subject"].dropna():
                    if isinstance(subjects, list):
                        subjects_list.extend(subjects[:3])  # Take first 3 subjects
                    elif isinstance(subjects, str):
                        subjects_list.append(subjects)
                
                if subjects_list:
                    subject_counts = pd.Series(subjects_list).value_counts().head(15)
                    fig_subjects = px.treemap(
                        names=subject_counts.index,
                        values=subject_counts.values,
                        title="📖 Temas/Géneros más Populares"
                    )
                    fig_subjects.update_layout(
                        font=dict(size=12, color='#2c3e50'),
                        title_font=dict(size=16, color='#2c3e50')
                    )
                    st.plotly_chart(fig_subjects, use_container_width=True)
                        
    except Exception as e:
        st.error(f"Error creating books charts: {e}")

def display_dag_status(dag_runs):
    """Display DAG runs with status indicators"""
    try:
        if dag_runs:
            df_runs = pd.DataFrame(dag_runs)
            
            # Add status colors
            def get_status_style(state):
                if state == "success":
                    return "status-success"
                elif state == "failed":
                    return "status-error"
                elif state == "running":
                    return "status-warning"
                else:
                    return ""
            
            # Display recent runs
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.write("**Execution Date**")
            with col2:
                st.write("**State**")
            with col3:
                st.write("**Duration**")
            
            for _, run in df_runs.head(5).iterrows():
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    try:
                        execution_date = pd.to_datetime(run.get('execution_date', ''), errors='coerce')
                        if pd.notna(execution_date):
                            st.write(execution_date.strftime('%Y-%m-%d %H:%M'))
                        else:
                            st.write("Invalid date")
                    except:
                        st.write("N/A")
                
                with col2:
                    state = run.get('state', 'unknown')
                    st.markdown(f'<span class="{get_status_style(state)}">{state.upper()}</span>', 
                               unsafe_allow_html=True)
                
                with col3:
                    try:
                        start_date = run.get('start_date')
                        end_date = run.get('end_date')
                        if start_date and end_date:
                            start = pd.to_datetime(start_date, errors='coerce')
                            end = pd.to_datetime(end_date, errors='coerce')
                            if pd.notna(start) and pd.notna(end):
                                duration = end - start
                                st.write(str(duration).split('.')[0])  # Remove microseconds
                            else:
                                st.write("-")
                        else:
                            st.write("-")
                    except:
                        st.write("-")
    except Exception as e:
        st.error(f"Error displaying DAG status: {e}")

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 ETL Monitoring Dashboard</h1>
        <p>Monitor your data pipelines and ETL processes in real-time</p>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.header("🔧 Connection Status")
        
        if st.button("🔄 Test Airflow Connection"):
            with st.spinner("Testing connection..."):
                status_code, response_text = test_airflow_connection()
                if status_code == 200:
                    st.success("✅ Airflow Connected")
                    try:
                        with st.expander("Version Info"):
                            st.json(json.loads(response_text))
                    except:
                        st.info("Connection successful but couldn't parse version info")
                else:
                    st.error(f"❌ Connection Failed ({status_code})")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.header("⚙️ Settings")
        limit = st.slider("Records to display:", 5, 100, 20, step=5)
        show_raw_data = st.checkbox("Show raw data tables", value=True)
        
        # Manual refresh button
        if st.button("🔄 Refresh All Data"):
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Database Statistics
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.header("📈 Database Stats")
        
        try:
            with st.spinner("Loading stats..."):
                stats = get_database_stats()
                
                for db_name, db_stats in stats.items():
                    count = db_stats.get('count', 0)
                    st.metric(
                        label=f"{db_name.title()} Records",
                        value=count if count is not None else 0
                    )
        except Exception as e:
            st.error(f"Error loading stats: {e}")
        st.markdown('</div>', unsafe_allow_html=True)

    # Main content
    st.markdown("## 📊 Latest ETL Data")

    # Create tabs for better organization
    tab1, tab2, tab3, tab4 = st.tabs(["🌤️ Weather", "🎓 Universities", "📚 Books", "⚙️ Airflow"])

    with tab1:
        st.markdown('<div class="data-section">', unsafe_allow_html=True)
        weather_docs = fetch_data("weather_db", "forecast", limit)
        
        if weather_docs:
            df_weather = pd.json_normalize(weather_docs).drop(columns=["_id"], errors="ignore")
            
            # Advanced filters for weather data
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Date range filter
                if "fetched_at" in df_weather.columns:
                    df_weather["fetched_at"] = pd.to_datetime(df_weather["fetched_at"], errors='coerce')
                    df_weather_clean = df_weather.dropna(subset=['fetched_at'])
                    
                    if len(df_weather_clean) > 0:
                        min_date = df_weather_clean["fetched_at"].min().date()
                        max_date = df_weather_clean["fetched_at"].max().date()
                        
                        date_range = st.date_input(
                            "📅 Date Range:",
                            value=(min_date, max_date),
                            min_value=min_date,
                            max_value=max_date
                        )
                        
                        if len(date_range) == 2:
                            start_date, end_date = date_range
                            date_mask = (df_weather_clean["fetched_at"].dt.date >= start_date) & \
                                       (df_weather_clean["fetched_at"].dt.date <= end_date)
                            df_weather = df_weather_clean[date_mask]
                        else:
                            df_weather = df_weather_clean
                    else:
                        st.warning("No valid dates found in weather data")
            
            with col2:
                # Temperature range filter
                if "temperature" in df_weather.columns:
                    temp_data = df_weather["temperature"].dropna()
                    if len(temp_data) > 0:
                        min_temp, max_temp = float(temp_data.min()), float(temp_data.max())
                        temp_range = st.slider(
                            "🌡️ Temperature Range (°C):",
                            min_value=min_temp,
                            max_value=max_temp,
                            value=(min_temp, max_temp),
                            step=0.1
                        )
                        
                        temp_mask = (df_weather["temperature"] >= temp_range[0]) & \
                                   (df_weather["temperature"] <= temp_range[1])
                        df_weather = df_weather[temp_mask]
            
            with col3:
                # Weather condition filter if available
                if "condition" in df_weather.columns:
                    conditions = ["All"] + sorted(df_weather["condition"].dropna().unique().tolist())
                    selected_condition = st.selectbox("☁️ Weather Condition:", conditions)
                    
                    if selected_condition != "All":
                        df_weather = df_weather[df_weather["condition"] == selected_condition]
                elif "description" in df_weather.columns:
                    descriptions = ["All"] + sorted(df_weather["description"].dropna().unique().tolist())
                    selected_description = st.selectbox("📝 Weather Description:", descriptions)
                    
                    if selected_description != "All":
                        df_weather = df_weather[df_weather["description"] == selected_description]
            
            # Display filter results metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Filtered Records", len(df_weather))
            with col2:
                if "temperature" in df_weather.columns and len(df_weather) > 0:
                    try:
                        avg_temp = df_weather["temperature"].mean()
                        st.metric("Avg Temperature", f"{avg_temp:.1f}°C")
                    except:
                        st.metric("Avg Temperature", "N/A")
                else:
                    st.metric("Avg Temperature", "N/A")
            with col3:
                if "humidity" in df_weather.columns and len(df_weather) > 0:
                    try:
                        avg_humidity = df_weather["humidity"].mean()
                        st.metric("Avg Humidity", f"{avg_humidity:.1f}%")
                    except:
                        st.metric("Avg Humidity", "N/A")
                else:
                    st.metric("Avg Humidity", "N/A")
            with col4:
                try:
                    if "fetched_at" in df_weather.columns and len(df_weather) > 0:
                        last_update = pd.to_datetime(df_weather["fetched_at"], errors='coerce').max()
                        if pd.notna(last_update):
                            st.metric("Last Update", last_update.strftime('%m/%d %H:%M'))
                        else:
                            st.metric("Last Update", "N/A")
                    else:
                        st.metric("Last Update", "N/A")
                except:
                    st.metric("Last Update", "N/A")
            
            # Charts with filtered data
            create_weather_charts(df_weather)
            
            # Enhanced data table
            if show_raw_data and len(df_weather) > 0:
                with st.expander("📋 Filtered Weather Data"):
                    # Time period analysis
                    if "fetched_at" in df_weather.columns:
                        df_weather_display = df_weather.copy()
                        df_weather_display["hour"] = pd.to_datetime(df_weather_display["fetched_at"]).dt.hour
                        df_weather_display["day_of_week"] = pd.to_datetime(df_weather_display["fetched_at"]).dt.day_name()
                        
                        # Show hourly/daily patterns
                        col1, col2 = st.columns(2)
                        with col1:
                            if len(df_weather_display) > 1:
                                hourly_avg = df_weather_display.groupby("hour")["temperature"].mean() if "temperature" in df_weather_display.columns else None
                                if hourly_avg is not None and len(hourly_avg) > 1:
                                    st.write("**Hourly Temperature Pattern:**")
                                    st.line_chart(hourly_avg)
                        
                        with col2:
                            if "day_of_week" in df_weather_display.columns and len(df_weather_display) > 1:
                                daily_counts = df_weather_display["day_of_week"].value_counts()
                                if len(daily_counts) > 1:
                                    st.write("**Data Points by Day:**")
                                    st.bar_chart(daily_counts)
                    
                    st.write(f"Showing {len(df_weather)} weather records")
                    
                    # Select columns to display
                    available_cols = df_weather.columns.tolist()
                    priority_cols = ["fetched_at", "temperature", "humidity", "condition", "description"]
                    default_cols = [col for col in priority_cols if col in available_cols][:5]
                    
                    display_cols = st.multiselect(
                        "Select columns to display:",
                        available_cols,
                        default=default_cols if default_cols else available_cols[:5]
                    )
                    
                    if display_cols:
                        st.dataframe(df_weather[display_cols].sort_values("fetched_at", ascending=False), use_container_width=True)
                    else:
                        st.dataframe(df_weather.sort_values("fetched_at", ascending=False), use_container_width=True)
        else:
            st.info("❌ No weather data available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.markdown('<div class="data-section">', unsafe_allow_html=True)
        uni_docs = fetch_data("universities_db", "universities", limit)
        
        if uni_docs:
            df_uni = pd.json_normalize(uni_docs).drop(columns=["_id"], errors="ignore")
            
            # Advanced filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                countries = ["All"] + sorted(df_uni["country"].dropna().unique().tolist()) if "country" in df_uni.columns else ["All"]
                selected_country = st.selectbox("🌍 Filter by Country:", countries)
            
            with col2:
                # State/Region filter if available
                if "state-province" in df_uni.columns:
                    df_temp = df_uni if selected_country == "All" else df_uni[df_uni["country"] == selected_country]
                    states = ["All"] + sorted(df_temp["state-province"].dropna().unique().tolist())
                    selected_state = st.selectbox("🏛️ Filter by State/Region:", states)
                else:
                    selected_state = "All"
            
            with col3:
                # Domain type filter
                if "domains" in df_uni.columns:
                    domain_types = ["All", ".edu", ".ac", ".org", ".com", "Others"]
                    selected_domain = st.selectbox("🌐 Filter by Domain Type:", domain_types)
                else:
                    selected_domain = "All"
            
            # Apply filters progressively
            df_filtered = df_uni.copy()
            
            if selected_country != "All":
                df_filtered = df_filtered[df_filtered["country"] == selected_country]
            
            if selected_state != "All" and "state-province" in df_filtered.columns:
                df_filtered = df_filtered[df_filtered["state-province"] == selected_state]
            
            if selected_domain != "All" and "domains" in df_filtered.columns:
                if selected_domain == "Others":
                    # Filter for domains that are not .edu, .ac, .org, .com
                    mask = df_filtered["domains"].apply(lambda x: 
                        isinstance(x, list) and len(x) > 0 and 
                        not any(domain.endswith(('.edu', '.ac', '.org', '.com')) for domain in x if domain)
                    )
                    df_filtered = df_filtered[mask]
                else:
                    # Filter for specific domain type
                    mask = df_filtered["domains"].apply(lambda x: 
                        isinstance(x, list) and len(x) > 0 and 
                        any(domain.endswith(selected_domain) for domain in x if domain)
                    )
                    df_filtered = df_filtered[mask]
            
            # Display filter results metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Filtered Results", len(df_filtered))
            with col2:
                if "country" in df_filtered.columns:
                    unique_countries = df_filtered["country"].nunique()
                    st.metric("Countries", unique_countries)
            with col3:
                if "state-province" in df_filtered.columns:
                    unique_states = df_filtered["state-province"].nunique()
                    st.metric("States/Regions", unique_states)
            with col4:
                total_percentage = (len(df_filtered) / len(df_uni) * 100) if len(df_uni) > 0 else 0
                st.metric("% of Total", f"{total_percentage:.1f}%")
            
            # Charts with filtered data
            create_universities_charts(df_filtered)
            
            # Enhanced data table with search
            if show_raw_data:
                with st.expander("📋 Filtered Universities Data"):
                    # Add search functionality
                    search_term = st.text_input("🔍 Search universities:", placeholder="Type university name...")
                    
                    if search_term:
                        if "name" in df_filtered.columns:
                            search_mask = df_filtered["name"].str.contains(search_term, case=False, na=False)
                            df_display = df_filtered[search_mask]
                        else:
                            df_display = df_filtered
                    else:
                        df_display = df_filtered
                    
                    st.write(f"Showing {len(df_display)} universities")
                    
                    # Select columns to display
                    available_cols = df_display.columns.tolist()
                    display_cols = st.multiselect(
                        "Select columns to display:",
                        available_cols,
                        default=available_cols[:5] if len(available_cols) >= 5 else available_cols
                    )
                    
                    if display_cols:
                        st.dataframe(df_display[display_cols], use_container_width=True)
                    else:
                        st.dataframe(df_display, use_container_width=True)
        else:
            st.info("❌ No university data available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab3:
        st.markdown('<div class="data-section">', unsafe_allow_html=True)
        book_docs = fetch_data("books_db", "books", limit)
        
        if book_docs:
            df_books = pd.json_normalize(book_docs).drop(columns=["_id"], errors="ignore")
            
            # Advanced filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                authors = ["All"] + sorted(df_books["author"].dropna().unique().tolist()) if "author" in df_books.columns else ["All"]
                selected_author = st.selectbox("✍️ Filter by Author:", authors)
            
            with col2:
                # Year range filter
                if "first_publish_year" in df_books.columns:
                    valid_years = df_books["first_publish_year"].dropna()
                    valid_years = valid_years[(valid_years >= 1800) & (valid_years <= 2024)]
                    
                    if len(valid_years) > 0:
                        min_year, max_year = int(valid_years.min()), int(valid_years.max())
                        year_range = st.slider(
                            "📅 Publication Year Range:",
                            min_value=min_year,
                            max_value=max_year,
                            value=(min_year, max_year),
                            step=1
                        )
                    else:
                        year_range = (1800, 2024)
                else:
                    year_range = (1800, 2024)
            
            with col3:
                # Subject/Genre filter if available
                if "subject" in df_books.columns:
                    # Extract unique subjects
                    all_subjects = set()
                    for subjects in df_books["subject"].dropna():
                        if isinstance(subjects, list):
                            all_subjects.update(subjects[:3])  # Take first 3 subjects
                        elif isinstance(subjects, str):
                            all_subjects.add(subjects)
                    
                    subject_options = ["All"] + sorted(list(all_subjects))[:50]  # Limit to 50 most common
                    selected_subject = st.selectbox("📚 Filter by Subject:", subject_options)
                else:
                    selected_subject = "All"
            
            # Apply filters
            df_filtered = df_books.copy()
            
            if selected_author != "All":
                df_filtered = df_filtered[df_filtered["author"] == selected_author]
            
            if "first_publish_year" in df_filtered.columns:
                df_filtered = df_filtered[
                    (df_filtered["first_publish_year"] >= year_range[0]) & 
                    (df_filtered["first_publish_year"] <= year_range[1])
                ]
            
            if selected_subject != "All" and "subject" in df_filtered.columns:
                mask = df_filtered["subject"].apply(lambda x: 
                    (isinstance(x, list) and selected_subject in x) or 
                    (isinstance(x, str) and selected_subject == x)
                )
                df_filtered = df_filtered[mask]
            
            # Display filter results metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Filtered Results", len(df_filtered))
            with col2:
                if "author" in df_filtered.columns:
                    unique_authors = df_filtered["author"].nunique()
                    st.metric("Authors", unique_authors)
            with col3:
                if "first_publish_year" in df_filtered.columns:
                    year_span = df_filtered["first_publish_year"].max() - df_filtered["first_publish_year"].min()
                    st.metric("Year Span", f"{int(year_span) if pd.notna(year_span) else 0} years")
            with col4:
                total_percentage = (len(df_filtered) / len(df_books) * 100) if len(df_books) > 0 else 0
                st.metric("% of Total", f"{total_percentage:.1f}%")
            
            # Charts with filtered data
            create_books_charts(df_filtered)
            
            # Enhanced data table with search
            if show_raw_data:
                with st.expander("📋 Filtered Books Data"):
                    # Add search functionality
                    search_term = st.text_input("🔍 Search books:", placeholder="Type book title or author...")
                    
                    if search_term:
                        search_cols = ["title", "author"] if "title" in df_filtered.columns else ["author"] if "author" in df_filtered.columns else []
                        if search_cols:
                            search_mask = df_filtered[search_cols].apply(
                                lambda x: x.astype(str).str.contains(search_term, case=False, na=False)
                            ).any(axis=1)
                            df_display = df_filtered[search_mask]
                        else:
                            df_display = df_filtered
                    else:
                        df_display = df_filtered
                    
                    st.write(f"Showing {len(df_display)} books")
                    
                    # Select columns to display
                    available_cols = df_display.columns.tolist()
                    priority_cols = ["title", "author", "first_publish_year", "subject"]
                    default_cols = [col for col in priority_cols if col in available_cols][:4]
                    
                    display_cols = st.multiselect(
                        "Select columns to display:",
                        available_cols,
                        default=default_cols if default_cols else available_cols[:4]
                    )
                    
                    if display_cols:
                        st.dataframe(df_display[display_cols], use_container_width=True)
                    else:
                        st.dataframe(df_display, use_container_width=True)
        else:
            st.info("❌ No book data available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab4:
        st.markdown('<div class="data-section">', unsafe_allow_html=True)
        
        # DAG Selection
        dag_list = ["weather_forecast_dag", "universities_dag", "open_library_dag"]
        
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_dag = st.selectbox("🔧 Select a DAG", dag_list)
        with col2:
            if st.button("🔄 Refresh DAG Runs"):
                st.rerun()
        
        # Fetch and display DAG runs
        with st.spinner("Loading DAG runs..."):
            runs = get_dag_runs(selected_dag)
            
            if runs:
                st.subheader(f"📋 Recent runs for {selected_dag}")
                display_dag_status(runs)
                
                # Link to Airflow UI
                st.markdown(f"""
                <div style="text-align: center; margin-top: 2rem;">
                    <a href="{AIRFLOW_UI_URL}/dags/{selected_dag}/grid" target="_blank" 
                       style="background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); 
                              color: white; padding: 10px 20px; text-decoration: none; 
                              border-radius: 8px; display: inline-block; font-weight: bold;
                              box-shadow: 0 4px 16px rgba(238,90,36,0.3);">
                        🚀 Open Airflow UI
                    </a>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("❌ No DAG runs found or unable to fetch.")
        
        st.markdown('</div>', unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>ETL Monitoring Dashboard | Last updated: {} | 
        <a href="https://github.com" target="_blank">📚 Documentation</a></p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

if __name__ == "__main__":
    main()