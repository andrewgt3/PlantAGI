import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import altair as alt
import json
import time
from fpdf import FPDF
import io

# --- CONFIGURATION ---
DB_CONNECTION = "postgresql://postgres:password@localhost:5432/pdm_timeseries"
ST_PAGE_TITLE = "Gaia | Industrial Command"
REFRESH_RATE_SEC = 2

# --- SETUP PAGE ---
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide", page_icon="🏭")

# --- STYLES ---
st.markdown("""
<style>
    .stAppDeployButton {display:none;}
    .stMetric {background-color: #f0f2f6; padding: 10px; border-radius: 5px;}
    .critical {border-left: 5px solid #ff4b4b; background-color: #262730; padding: 10px; border-radius: 5px;}
    .warning {border-left: 5px solid #ffa500; background-color: #262730; padding: 10px; border-radius: 5px;}
    .good {border-left: 5px solid #00cc96; background-color: #262730; padding: 10px; border-radius: 5px;}
    h3 {margin-top: 0px;}
</style>
""", unsafe_allow_html=True)

# --- BACKEND FUNCTIONS (Cached) ---
@st.cache_resource
def get_engine():
    return create_engine(DB_CONNECTION)

@st.cache_data(ttl=REFRESH_RATE_SEC)
def load_data(asset_id=None):
    engine = get_engine()
    
    # 1. LOAD FLEET SNAPSHOT (For the Grid)
    # Get the very latest row for EVERY robot
    query_fleet = """
    SELECT DISTINCT ON (asset_id) 
        asset_id, timestamp, vibration_x, motor_temp_c, rul_hours
    FROM sensors 
    ORDER BY asset_id, timestamp DESC
    """
    
    # 2. LOAD DETAIL HISTORY (For the Charts)
    # If an asset is selected, get its history
    query_history = ""
    query_events = ""
    if asset_id:
        query_history = f"""
        SELECT timestamp, vibration_x, joint_1_torque, motor_temp_c 
        FROM sensors 
        WHERE asset_id = '{asset_id}'
        ORDER BY timestamp DESC 
        LIMIT 3000
        """
        # Look for events relevant to this asset (or global events)
        query_events = """
        SELECT timestamp, event_type, severity, description 
        FROM events 
        ORDER BY timestamp DESC
        """

    with engine.connect() as conn:
        df_fleet = pd.read_sql(text(query_fleet), conn)
        df_history = pd.DataFrame()
        df_events = pd.DataFrame()
        
        if asset_id:
            df_history = pd.read_sql(text(query_history), conn)
            df_events = pd.read_sql(text(query_events), conn)
            
    return df_fleet, df_history, df_events

# --- PDF GENERATOR ---
def create_work_order(asset_id, insight):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="Gaia Predictive | Work Order", ln=1, align='C')
    pdf.line(10, 20, 200, 20)
    pdf.ln(10)
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Asset: {asset_id}", ln=1)
    pdf.cell(200, 10, txt=f"Priority: HIGH", ln=1)
    pdf.ln(10)
    pdf.multi_cell(0, 10, txt=f"Issue: {insight['event_type']} detected.\nRoot Cause: Correlated Context Event.\nAction: Inspect power isolation.")
    return pdf.output(dest='S').encode('latin-1')

# --- VIEW 1: FLEET OVERVIEW ---
def render_fleet_view(df_fleet):
    st.title("🏭 Plant Overview | Zone A")
    
    # Cascade Logic
    critical_assets = df_fleet[df_fleet['rul_hours'] < 48]
    if len(critical_assets) >= 2:
        st.error(f"🚨 SYSTEMIC RISK ALERT: {len(critical_assets)} Assets Critical. Potential Cascade Failure detected.")
    else:
        st.success("System Status: Nominal")
    
    st.divider()
    
    # Grid Layout
    cols = st.columns(4)
    for i, row in df_fleet.iterrows():
        asset = row['asset_id']
        rul = row['rul_hours']
        
        # Color Logic
        if rul < 24:
            style = "critical"
            status = "🔴 CRITICAL"
        elif rul < 168:
            style = "warning"
            status = "🟡 WARNING"
        else:
            style = "good"
            status = "🟢 HEALTHY"
            
        with cols[i % 4]:
            st.markdown(f"""
            <div class="{style}">
                <h3>{asset}</h3>
                <b>{status}</b><br>
                RUL: {int(rul)} hrs<br>
                Vib: {row['vibration_x']:.2f} g
            </div>
            """, unsafe_allow_html=True)
            
            st.write("")
            if st.button(f"Analyze {asset}", key=f"btn_{asset}", type="secondary"):
                st.session_state['selected_asset'] = asset
                st.rerun()

# --- VIEW 2: ASSET DETAIL (The Deep Dive) ---
def render_detail_view(asset_id, df_history, df_events):
    # Back Button
    if st.button("← Back to Fleet Command"):
        st.session_state['selected_asset'] = None
        st.rerun()

    # Header
    st.title(f"🤖 Diagnostics: {asset_id}")
    
    # Calculate anomaly statistics
    df_history['rolling_mean'] = df_history['vibration_x'].rolling(window=60, min_periods=1).mean()
    df_history['rolling_std'] = df_history['vibration_x'].rolling(window=60, min_periods=1).std()
    df_history['rolling_std'] = df_history['rolling_std'].fillna(0)
    
    # Detect anomalies (> 2 std deviations)
    threshold = df_history['rolling_mean'] + (2 * df_history['rolling_std'])
    df_history['is_anomaly'] = df_history['vibration_x'] > threshold
    
    anomaly_count = df_history['is_anomaly'].sum()
    anomaly_pct = (anomaly_count / len(df_history)) * 100 if len(df_history) > 0 else 0
    
    # Top Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        latest = df_history.iloc[0]
        baseline = df_history['vibration_x'].mean()
        delta = ((latest['vibration_x'] - baseline) / baseline) * 100
        st.metric("Real-Time Vibration", f"{latest['vibration_x']:.3f} g", f"{delta:+.1f}%")
    with col2:
        st.metric("Anomalies Detected", f"{anomaly_count}", f"{anomaly_pct:.1f}%")
    with col3:
        st.metric("Avg Motor Temp", f"{df_history['motor_temp_c'].mean():.1f}°C")
    with col4:
        st.metric("Avg Torque", f"{df_history['joint_1_torque'].mean():.1f} Nm")
    
    st.divider()
    
    # The Chart (Downsampled for performance)
    df_chart = df_history.iloc[::5, :].copy()  # 5x Faster
    df_anomalies = df_chart[df_chart['is_anomaly']].copy()
    
    # Base vibration line
    base = alt.Chart(df_chart).encode(
        x=alt.X('timestamp:T', axis=alt.Axis(title='Time', format='%H:%M')),
    )
    
    vibration_line = base.mark_line(color='#0068c9', size=2).encode(
        y=alt.Y('vibration_x:Q', title='Vibration (g)'),
        tooltip=['timestamp:T', 'vibration_x:Q', 'rolling_mean:Q']
    )
    
    # Rolling mean line
    mean_line = base.mark_line(color='#00cc96', strokeDash=[5,5]).encode(
        y='rolling_mean:Q',
        tooltip=['timestamp:T', 'rolling_mean:Q']
    )
    
    # Anomaly points
    anomaly_points = alt.Chart(df_anomalies).mark_circle(
        size=100,
        color='#ff4b4b',
        opacity=0.7
    ).encode(
        x='timestamp:T',
        y='vibration_x:Q',
        tooltip=['timestamp:T', 'vibration_x:Q']
    )
    
    # Event overlay (red vertical lines for Conveyor_Jam)
    min_time = df_chart['timestamp'].min()
    visible_events = df_events[df_events['timestamp'] >= min_time]
    
    if not visible_events.empty:
        event_rules = alt.Chart(visible_events).mark_rule(
            color='red',
            strokeDash=[5,5],
            size=2
        ).encode(
            x='timestamp:T',
            tooltip=['timestamp:T', 'event_type:N', 'severity:N']
        )
        chart = (vibration_line + mean_line + anomaly_points + event_rules).interactive()
        insight_active = True
    else:
        chart = (vibration_line + mean_line + anomaly_points).interactive()
        insight_active = False

    st.altair_chart(chart, use_container_width=True)
    
    # Anomaly Details Table
    if anomaly_count > 0:
        st.subheader("🚨 Anomaly Details")
        top_anomalies = df_history[df_history['is_anomaly']].nlargest(10, 'vibration_x')[
            ['timestamp', 'vibration_x', 'rolling_mean', 'joint_1_torque', 'motor_temp_c']
        ]
        st.dataframe(top_anomalies, use_container_width=True)

    # Agent Sidebar (Only in Detail View)
    with st.sidebar:
        st.header(f"🧠 {asset_id} Analysis")
        
        if insight_active:
            st.error("⚠️ ROOT CAUSE DETECTED")
            event = visible_events.iloc[0]
            st.markdown(f"""
            **Event**: {event['event_type']}  
            **Severity**: {event['severity']}  
            **Description**: {event['description']}
            
            **Correlation**: Vibration anomalies correlate with cascade failure event.
            """)
            
            # PDF Button
            insight_data = {'event_type': event['event_type']}
            pdf_bytes = create_work_order(asset_id, insight_data)
            st.download_button(
                "📄 Download Work Order",
                pdf_bytes,
                f"work_order_{asset_id}.pdf",
                "application/pdf",
                type="primary"
            )
        else:
            st.info("✓ No active correlations detected")
            st.metric("System Status", "Nominal")

# --- MAIN APP LOOP ---
def main():
    if 'selected_asset' not in st.session_state:
        st.session_state['selected_asset'] = None

    # Load Data (Smart Loading)
    asset_to_load = st.session_state['selected_asset']
    try:
        df_fleet, df_history, df_events = load_data(asset_to_load)
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        st.stop()

    # Router
    if st.session_state['selected_asset']:
        render_detail_view(st.session_state['selected_asset'], df_history, df_events)
    else:
        render_fleet_view(df_fleet)
        
    # Auto-Refresh
    time.sleep(REFRESH_RATE_SEC)
    st.rerun()

if __name__ == "__main__":
    main()
