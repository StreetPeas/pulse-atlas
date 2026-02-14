#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PY="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"

# 1) зависимости (streamlit + pandas)
"$PY" -m pip install --upgrade pip >/dev/null
"$PY" -m pip install streamlit pandas >/dev/null

# 2) пишем dashboard.py
cat > dashboard.py <<'PY'
#!/usr/bin/env python3
"""
Pulse Atlas Dashboard
Real-time system signal monitoring and early warning interface
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import streamlit as st
import pandas as pd

# ==================== CONFIG ====================

DB_PATH = "data/atlas.db"
REFRESH_INTERVAL = 60  # seconds

COLOR_MAP = {
    "🔴": "#FF4444",  # Red - High risk
    "🟡": "#FFB84D",  # Yellow - Watch
    "🟢": "#4CAF50",  # Green - Progress
    "⚫": "#666666",  # Black - Neutral
}

# ==================== DATA ====================

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_data():
    """Load all signals from database"""
    if not Path(DB_PATH).exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT 
            id,
            ts,
            source,
            title,
            url,
            color,
            score,
            label,
            rationale,
            summary
        FROM signals
        ORDER BY id DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    return df


def get_stats(df):
    """Calculate dashboard statistics"""
    if df.empty:
        return {
            "total": 0,
            "sources": 0,
            "red": 0,
            "yellow": 0,
            "green": 0,
            "neutral": 0,
            "avg_score": 0.0,
            "last_24h": 0,
        }

    now = pd.Timestamp.now(tz='UTC')
    last_24h = df[df['ts'] > (now - timedelta(hours=24))]
    color_counts = df['color'].value_counts().to_dict()

    return {
        "total": len(df),
        "sources": df['source'].nunique(),
        "red": color_counts.get('🔴', 0),
        "yellow": color_counts.get('🟡', 0),
        "green": color_counts.get('🟢', 0),
        "neutral": color_counts.get('⚫', 0),
        "avg_score": float(df['score'].mean()) if 'score' in df else 0.0,
        "last_24h": len(last_24h),
    }


def shorten_url(url, max_len=40):
    """Shorten URL for display"""
    if not url or len(url) <= max_len:
        return url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        path = parsed.path[:20] if parsed.path else ""
        return f"{domain}{path}..."
    except Exception:
        return url[:max_len] + "..."


def shorten_source(source, max_len=30):
    """Shorten source name for display"""
    if not source or len(source) <= max_len:
        return source
    if '/' in source:
        return source.split('/')[-1][:max_len]
    return source[:max_len]


# ==================== UI ====================

def main():
    st.set_page_config(
        page_title="Pulse Atlas",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📡 Pulse Atlas")
    st.caption("Real-time system signal monitoring and early warning")

    df = load_data()

    if df.empty:
        st.warning("⚠️ No data available. Run `python3 rss_fetch.py` to collect signals.")
        return

    stats = get_stats(df)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Signals", f"{stats['total']:,}")
    col2.metric("Sources", stats['sources'])
    col3.metric("🔴 Risk", stats['red'])
    col4.metric("🟡 Watch", stats['yellow'])
    col5.metric("🟢 Progress", stats['green'])
    col6.metric("Last 24h", stats['last_24h'])

    st.divider()

    with st.sidebar:
        st.header("🔍 Filters")

        color_options = ["All"] + sorted(df['color'].dropna().unique().tolist())
        selected_color = st.selectbox("Color", color_options)

        source_options = ["All"] + sorted(df['source'].dropna().unique().tolist())
        selected_source = st.selectbox("Source", source_options, format_func=lambda x: shorten_source(x, 40))

        score_range = st.slider(
            "Score Range",
            min_value=0.0,
            max_value=1.0,
            value=(0.0, 1.0),
            step=0.05,
        )

        time_options = {
            "All Time": None,
            "Last 24 Hours": 24,
            "Last 7 Days": 168,
            "Last 30 Days": 720,
        }
        selected_time = st.selectbox("Time Period", list(time_options.keys()))
        hours_back = time_options[selected_time]

        st.divider()

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        st.caption(f"Auto-refresh: {REFRESH_INTERVAL}s")

    filtered_df = df.copy()

    if selected_color != "All":
        filtered_df = filtered_df[filtered_df['color'] == selected_color]

    if selected_source != "All":
        filtered_df = filtered_df[filtered_df['source'] == selected_source]

    filtered_df = filtered_df[
        (filtered_df['score'] >= score_range[0]) &
        (filtered_df['score'] <= score_range[1])
    ]

    if hours_back:
        cutoff = pd.Timestamp.now(tz='UTC') - timedelta(hours=hours_back)
        filtered_df = filtered_df[filtered_df['ts'] > cutoff]

    tab1, tab2, tab3, tab4 = st.tabs(["🚨 High Priority", "📊 All Signals", "📈 Analytics", "ℹ️ About"])

    with tab1:
        st.subheader("🔴 High Risk Signals")
        red_signals = filtered_df[filtered_df['color'] == '🔴'].sort_values('score', ascending=False)

        if red_signals.empty:
            st.success("✅ No high-risk signals detected")
        else:
            st.caption(f"Showing {len(red_signals)} risk signals")

            for i, (_, row) in enumerate(red_signals.head(20).iterrows()):
                with st.expander(f"🔴 [{row['score']:.2f}] {row['title']}", expanded=(i == 0)):
                    c1, c2 = st.columns([3, 1])

                    with c1:
                        st.markdown(f"**Source:** {shorten_source(row['source'])}")
                        if row.get('url'):
                            st.markdown(f"**URL:** [{shorten_url(row['url'])}]({row['url']})")
                        if row.get('summary'):
                            st.markdown(f"**Summary:** {str(row['summary'])[:200]}...")
                        if row.get('rationale'):
                            st.caption(f"*{row['rationale']}*")

                    with c2:
                        st.metric("Score", f"{row['score']:.2f}")
                        if row.get('label'):
                            st.caption(f"Label: {row['label']}")
                        if pd.notna(row.get('ts')):
                            st.caption(f"{row['ts'].strftime('%Y-%m-%d %H:%M')}")

        st.divider()

        st.subheader("🟢 Progress Signals")
        green_signals = filtered_df[filtered_df['color'] == '🟢'].sort_values('score', ascending=False)

        if green_signals.empty:
            st.info("No progress signals in current filter")
        else:
            st.caption(f"Showing {len(green_signals)} progress signals")
            for _, row in green_signals.head(10).iterrows():
                c1, c2 = st.columns([4, 1])
                c1.markdown(f"🟢 **{row['title']}**")
                if pd.notna(row.get('ts')):
                    c1.caption(f"{shorten_source(row['source'])} • {row['ts'].strftime('%Y-%m-%d')}")
                else:
                    c1.caption(f"{shorten_source(row['source'])}")
                c2.metric("", f"{row['score']:.2f}")

    with tab2:
        st.subheader(f"All Signals ({len(filtered_df)} total)")

        c1, c2 = st.columns([3, 1])
        with c1:
            display_limit = st.slider("Display limit", 10, 100, 50, 10)
        with c2:
            sort_by = st.selectbox("Sort by", ["Latest", "Score", "Title"])

        if sort_by == "Latest":
            display_df = filtered_df.head(display_limit)
        elif sort_by == "Score":
            display_df = filtered_df.sort_values('score', ascending=False).head(display_limit)
        else:
            display_df = filtered_df.sort_values('title').head(display_limit)

        for _, row in display_df.iterrows():
            c1, c2, c3 = st.columns([1, 6, 1])
            c1.markdown(f"### {row['color']}")
            c2.markdown(f"**{row['title']}**")
            meta_parts = [shorten_source(row['source'])]
            if pd.notna(row.get('ts')):
                meta_parts.append(row['ts'].strftime('%Y-%m-%d %H:%M'))
            if row.get('label'):
                meta_parts.append(str(row['label']))
            c2.caption(" • ".join(meta_parts))
            if row.get('url'):
                c2.caption(f"🔗 [{shorten_url(row['url'])}]({row['url']})")
            c3.metric("", f"{row['score']:.2f}")
            st.divider()

    with tab3:
        st.subheader("📈 System Analytics")
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("**Signal Distribution by Color**")
            color_data = filtered_df['color'].value_counts().reset_index()
            color_data.columns = ['Color', 'Count']
            st.bar_chart(color_data.set_index('Color'))

        with c2:
            st.markdown("**Top 10 Sources**")
            source_data = filtered_df['source'].value_counts().head(10).reset_index()
            source_data.columns = ['Source', 'Count']
            source_data['Source'] = source_data['Source'].apply(lambda x: shorten_source(x, 25))
            st.bar_chart(source_data.set_index('Source'))

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Score Distribution**")
            st.bar_chart(filtered_df['score'].value_counts().sort_index())

        with c2:
            st.markdown("**Label Distribution**")
            label_data = filtered_df['label'].value_counts().head(10).reset_index()
            label_data.columns = ['Label', 'Count']
            st.bar_chart(label_data.set_index('Label'))

        st.divider()

        if not filtered_df['ts'].isna().all():
            st.markdown("**Signal Timeline (Last 7 Days)**")
            recent = filtered_df[filtered_df['ts'] > (pd.Timestamp.now(tz='UTC') - timedelta(days=7))]
            if not recent.empty:
                timeline = recent.groupby(recent['ts'].dt.date).size().reset_index()
                timeline.columns = ['Date', 'Count']
                st.line_chart(timeline.set_index('Date'))

    with tab4:
        st.subheader("About Pulse Atlas")
        st.markdown(f"""
**Pulse Atlas** is an early warning system for technology ecosystem signals.

- **Total Signals**: {stats['total']:,}  
- **Active Sources**: {stats['sources']}  
- **Average Score**: {stats['avg_score']:.2f}  
- **Database**: `{DB_PATH}`  
- **Last Updated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""")
        with st.expander("🔧 Commands"):
            st.code("""
python3 rss_fetch.py
python3 score_signals.py
./run.sh
streamlit run dashboard.py
            """, language="bash")


if __name__ == "__main__":
    main()
PY

chmod +x dashboard.py

echo "OK: dashboard.py written -> $(pwd)/dashboard.py"
echo "OK: starting Streamlit..."
exec "$PY" -m streamlit run dashboard.py --server.port 8501 --server.address 127.0.0.1
