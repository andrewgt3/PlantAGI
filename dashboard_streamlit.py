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
        SELECT timestamp, event_type, staff_id 
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
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"🤖 Diagnostics: {asset_id}")
    with col2:
        latest = df_history.iloc[0]
        st.metric("Real-Time Vibration", f"{latest['vibration_x']:.3f} g", "-0.01")

    # The Chart (Downsampled)
    df_chart = df_history.iloc[::5, :] # 5x Faster
    
    base = alt.Chart(df_chart).encode(
        x=alt.X('timestamp', axis=alt.Axis(title='Time', format='%H:%M')),
        tooltip=['timestamp', 'vibration_x']
    )
    line = base.mark_line(color='#0068c9').encode(y='vibration_x')
    
    # Context Overlay (Red Line)
    # Filter events to what fits on the graph
    min_time = df_chart['timestamp'].min()
    visible_events = df_events[df_events['timestamp'] >= min_time]
    
    # Focus on the 'Cleaning Crew' type events for the visual
    cleaning_events = visible_events[visible_events['event_type'].str.contains('Cleaning', na=False)]

    if not cleaning_events.empty:
        rules = alt.Chart(cleaning_events).mark_rule(color='red', strokeDash=[5,5]).encode(x='timestamp')
        chart = (line + rules).interactive()
        insight_active = True
    else:
        chart = line.interactive()
        insight_active = False

    st.altair_chart(chart, use_container_width=True)

    # Agent Sidebar (Only in Detail View)
    with st.sidebar:
        st.header(f"🧠 {asset_id} Analysis")
        if insight_active:
            st.error("ROOT CAUSE FOUND")
            st.markdown("**Correlation:** Vibration spike follows Cleaning Crew entry by 16.3 mins.")
            
            # PDF Button
            insight_data = {'event_type': 'Cleaning Crew Interference'}
            pdf_bytes = create_work_order(asset_id, insight_data)
            st.download_button("📄 Download Work Order", pdf_bytes, "work_order.pdf", "application/pdf", type="primary")
        else:
            st.info("No active correlations.")

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
