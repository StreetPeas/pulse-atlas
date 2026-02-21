import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
import urllib.parse
from uuid import uuid4

# -------------------------
# Config
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "atlas.db"
REFRESH_SEC = 60

st.set_page_config(
    page_title="Pulse Atlas",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------
# Styles (no JS; stable)
# -------------------------
st.markdown(
    """
<style>
/* Background */
.stApp { background: radial-gradient(1200px 800px at 50% 0%, rgba(40,80,255,0.10), rgba(0,0,0,0.95) 70%), #000; }
.block-container { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1200px; }

/* Cards */
.pa-card {
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 18px;
  padding: 16px 18px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.35);
}
.pa-title { font-size: 34px; font-weight: 700; letter-spacing: 0.3px; margin: 0 0 2px 0; }
.pa-sub { opacity: 0.75; margin: 0 0 14px 0; }

/* Sections */
.pa-anchor { scroll-margin-top: 90px; }
.pa-section { margin-top: 18px; }
.pa-h2 { font-size: 22px; font-weight: 700; margin: 6px 0 10px 0; }
.pa-desc { opacity: 0.78; margin: 0 0 10px 0; }
.hr { height: 1px; background: rgba(255,255,255,0.10); margin: 22px 0; }

/* Planet nav (anchors, no JS) */
.pa-planets {
  position: fixed;
  right: 18px;
  top: 140px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  z-index: 9999;
}
.pa-planet {
  width: 44px;
  height: 44px;
  border-radius: 999px;
  display: grid;
  place-items: center;
  text-decoration: none !important;
  color: rgba(255,255,255,0.92) !important;
  border: 1px solid rgba(255,255,255,0.16);
  background: radial-gradient(circle at 30% 25%, rgba(255,255,255,0.18), rgba(255,255,255,0.05) 55%, rgba(0,0,0,0.45));
  box-shadow: 0 8px 22px rgba(0,0,0,0.45);
  transition: transform .12s ease, border-color .12s ease, background .12s ease;
}
.pa-planet:hover { transform: translateY(-1px) scale(1.02); border-color: rgba(255,255,255,0.30); }
.pa-planet:active { transform: translateY(0px) scale(0.99); }
.pa-planet small { font-size: 12px; opacity: 0.85; }

/* Sidebar */
section[data-testid="stSidebar"] { background: rgba(5,10,20,0.35); border-right: 1px solid rgba(255,255,255,0.08); }
</style>
""",
    unsafe_allow_html=True,
)

# -------------------------
# Data
# -------------------------
@st.cache_data(ttl=10, show_spinner=False)
def load_signals(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query("SELECT * FROM signals ORDER BY ts DESC", conn)
    finally:
        conn.close()
    return df



def load_actions(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query("SELECT * FROM actions ORDER BY priority DESC, ts DESC", conn)
    finally:
        conn.close()
    return df

def safe_col(df: pd.DataFrame, name: str, default=None):
    return df[name] if name in df.columns else default

def filter_df(df: pd.DataFrame, color: str, source: str, score_min: float, score_max: float, days: int) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    # ts parse (if exists)
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce", utc=True)
        if days is not None:
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=int(days))
            out = out[out["ts"] >= cutoff]

    if color != "All" and "color" in out.columns:
        out = out[out["color"] == color]

    if source != "All" and "source" in out.columns:
        out = out[out["source"] == source]

    if "score" in out.columns:
        out["score"] = pd.to_numeric(out["score"], errors="coerce")
        out = out[(out["score"] >= float(score_min)) & (out["score"] <= float(score_max))]

    return out

def metrics_block(df_all: pd.DataFrame, df_filtered: pd.DataFrame):
    a = len(df_all)
    f = len(df_filtered)
    mean_score = float(pd.to_numeric(df_filtered["score"], errors="coerce").mean()) if (not df_filtered.empty and "score" in df_filtered.columns) else 0.0

    last_ts = None
    if not df_filtered.empty and "ts" in df_filtered.columns:
        last_ts = pd.to_datetime(df_filtered["ts"], errors="coerce", utc=True).max()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown('<div class="pa-card">', unsafe_allow_html=True)
        st.markdown(f"**Signals (all)**  \n### {a}", unsafe_allow_html=True)
        st.caption(f"last: {last_ts.strftime('%Y-%m-%d %H:%M UTC') if last_ts is not None else '—'}")
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="pa-card">', unsafe_allow_html=True)
        st.markdown(f"**Signals (filtered)**  \n### {f}", unsafe_allow_html=True)
        st.caption("current view")
        st.markdown("</div>", unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="pa-card">', unsafe_allow_html=True)
        st.markdown(f"**Mean score**  \n### {mean_score:.3f}", unsafe_allow_html=True)
        st.caption("filtered")
        st.markdown("</div>", unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="pa-card">', unsafe_allow_html=True)
        st.markdown("**Core mode**  \n### OFF", unsafe_allow_html=True)
        st.caption("Bittensor, GAEA, Akash")
        st.markdown("</div>", unsafe_allow_html=True)

def table_view(df: pd.DataFrame, title: str):
    st.markdown(f'<div class="pa-h2">{title}</div>', unsafe_allow_html=True)
    if df.empty:
        st.info("Нет данных.")
        return
    # Keep a reasonable set of columns
    cols_pref = ["ts", "object", "color", "score", "label", "source", "title", "url"]
    cols = [c for c in cols_pref if c in df.columns] or list(df.columns)[:10]
    view = st.radio("View mode", ["Cards", "Table"], horizontal=True, key=f"view_mode_{title}")
    if view == "Cards":
        cards_view(df, title)
        return

    st.dataframe(df[cols], width="stretch", height=520)



def _pa_safe_str(x) -> str:
    try:
        if x is None:
            return ""
        return str(x)
    except Exception:
        return ""

def _pa_get(row, key: str):
    try:
        return row.get(key, "")
    except Exception:
        try:
            return getattr(row, key)
        except Exception:
            return ""

def _pa_url(row, key: str) -> str:
    return _pa_safe_str(_pa_get(row, key)).strip()

def _pa_actions_urls(row):
    src = _pa_url(row, "source")
    title = _pa_url(row, "title") or _pa_url(row, "label") or _pa_url(row, "object")
    url = _pa_url(row, "url")

    q_base = (title + " " + src).strip()
    if q_base:
        q_analytics = urllib.parse.quote_plus(q_base)
        q_fix = urllib.parse.quote_plus((q_base + " fix issue solution").strip())
        analytics = "https://www.google.com/search?q=" + q_analytics
        solution = "https://www.google.com/search?q=" + q_fix
    else:
        analytics = ""
        solution = ""

    return url, analytics, solution

def cards_view(df: pd.DataFrame, title: str, limit: int = 120):
    st.markdown(f'<div class="pa-h2">{title}</div>', unsafe_allow_html=True)
    if df.empty:
        st.info("Нет данных.")
        return

    d = df.head(int(limit)).copy()

    for _, row in d.iterrows():
        ts = _pa_url(row, "ts")
        obj = _pa_url(row, "object") or _pa_url(row, "label") or "—"
        color = _pa_url(row, "color")
        score = _pa_url(row, "score")
        src = _pa_url(row, "source")
        title_txt = _pa_url(row, "title")
        summary = _pa_url(row, "summary") or _pa_url(row, "text")

        url, analytics, solution = _pa_actions_urls(row)

        st.markdown('<div class="pa-card">', unsafe_allow_html=True)

        head = f"**{obj}**"
        if score:
            head += f" · score `{score}`"
        if color:
            head += f" · {color}"
        st.markdown(head)

        meta = []
        if ts: meta.append(ts)
        if src: meta.append(src)
        if title_txt: meta.append(title_txt)
        if meta:
            st.caption(" · ".join(meta[:3]))

        if summary:
            st.write(summary[:400] + ("…" if len(summary) > 400 else ""))

        c1, c2, c3 = st.columns(3)
        with c1:
            if url:
                st.link_button("Импульс", url)
            else:
                st.button("Импульс", disabled=True, key=f"nav_impulse_{uuid4().hex}")
        with c2:
            if analytics:
                st.link_button("Аналитика", analytics)
            else:
                st.button("Аналитика", disabled=True, key=f"nav_analytics_{uuid4().hex}")
        with c3:
            if solution:
                st.link_button("Решение", solution)
            else:
                st.button("Решение", disabled=True, key=f"nav_decision_{uuid4().hex}")

        st.markdown("</div>", unsafe_allow_html=True)

def priority_view(df: pd.DataFrame):
    st.markdown('<div class="pa-h2">High Priority</div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-desc">Быстрый риск / важное (🔴 сверху + top score)</div>', unsafe_allow_html=True)
    if df.empty:
        st.info("Нет данных.")
        return

    d = df.copy()
    if "score" in d.columns:
        d["score"] = pd.to_numeric(d["score"], errors="coerce")
    if "ts" in d.columns:
        d["ts"] = pd.to_datetime(d["ts"], errors="coerce", utc=True)

    red = d[d["color"] == "🔴"] if "color" in d.columns else d.iloc[0:0]
    top = d.sort_values("score", ascending=False).head(60) if "score" in d.columns else d.head(60)
    merged = pd.concat([red, top], ignore_index=True)
    # drop duplicates if possible
    key_cols = [c for c in ["ts", "source", "url", "title"] if c in merged.columns]
    if key_cols:
        merged = merged.drop_duplicates(subset=key_cols)

    table_view(merged, "Signals (priority)")



def actions_view(df: pd.DataFrame):
    st.markdown('<div class="pa-h2">Actions</div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-desc">Автогенерируемые задачи от decision_engine (monitor / investigate). Дедуп включён.</div>', unsafe_allow_html=True)
    if df is None or df.empty:
        st.info("Нет actions.")
        return

    cols_pref = ["ts", "status", "priority", "action_type", "title", "url", "signal_id", "last_error"]
    cols = [c for c in cols_pref if c in df.columns] or list(df.columns)[:10]

    view = st.radio("Actions view", ["Table", "Open only"], horizontal=True, key="actions_view_mode")
    d = df.copy()
    if view == "Open only" and "status" in d.columns:
        d = d[d["status"].fillna("open") == "open"]

    st.dataframe(d[cols], width="stretch", height=420)

def analytics_view(df: pd.DataFrame):
    st.markdown('<div class="pa-h2">Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-desc">Заготовка: графики подключим на следующем шаге.</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("Нет данных.")
        return

    # signals/day
    if "ts" in df.columns:
        dd = df.copy()
        dd["ts"] = pd.to_datetime(dd["ts"], errors="coerce", utc=True)
        dd = dd.dropna(subset=["ts"])
        dd["day"] = dd["ts"].dt.date
        sday = dd.groupby("day").size().reset_index(name="signals")
        st.line_chart(sday.set_index("day")["signals"], height=220)

    # activity by source
    if "source" in df.columns:
        ss = df.groupby("source").size().sort_values(ascending=False).head(12)
        st.bar_chart(ss, height=220)

def about_view(db_path: Path, df_all: pd.DataFrame):
    st.markdown('<div class="pa-h2">About</div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-desc">Техническая информация (чтобы не теряться).</div>', unsafe_allow_html=True)

    st.markdown('<div class="pa-card">', unsafe_allow_html=True)
    st.write("DB:", str(db_path))
    st.write("DB exists:", db_path.exists())
    st.write("Rows:", len(df_all))
    st.write("Auto-refresh:", f"{REFRESH_SEC}s")
    st.markdown("</div>", unsafe_allow_html=True)


def _pa_actions_ui_v2(row, df_ctx):
    """UI actions for a single signal row. No external windows."""
    import pandas as pd
    import streamlit as st

    def _get(k, default=""):
        try:
            if hasattr(row, "get"):
                v = row.get(k, default)
            else:
                v = getattr(row, k, default)
        except Exception:
            v = default
        return default if v is None else v

    sid = str(_get("id", ""))
    ts = str(_get("ts", ""))
    source = str(_get("source", ""))
    obj = str(_get("object", ""))
    title = str(_get("title", "")) or "(no title)"
    url = str(_get("url", ""))
    color = str(_get("color", ""))
    label = str(_get("label", ""))
    score = _get("score", None)
    rationale = str(_get("rationale", ""))
    summary = str(_get("summary", ""))
    impact_note = str(_get("impact_note", ""))
    raw = _get("raw", None)
    body = _get("body", None)
    meta = _get("meta", None)
    th = str(_get("t_horizon", ""))

    # --- session state (which panel is open under which card)
    st.session_state.setdefault("pa_open", {"sid": None, "tab": None})

    # --- buttons row
    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1:
        if st.button("Импульс", key=f"imp_{sid}"):
            st.session_state["pa_open"] = {"sid": sid, "tab": "impulse"}
            st.rerun()
    with c2:
        if st.button("Аналитика", key=f"ana_{sid}"):
            st.session_state["pa_open"] = {"sid": sid, "tab": "analytics"}
            st.rerun()
    with c3:
        if st.button("Решение", key=f"dec_{sid}"):
            st.session_state["pa_open"] = {"sid": sid, "tab": "decision"}
            st.rerun()
    with c4:
        if url:
            st.link_button("Источник", url, help="Открыть первоисточник")

    # --- render opened panel (only for this sid)
    st.session_state.setdefault("pa_open", {"sid": None, "tab": None})
    opened = st.session_state["pa_open"]
    if opened.get("sid") != sid:
        return

    tab = opened.get("tab")
    titles = {"impulse":"Импульс", "analytics":"Аналитика", "decision":"Решение"}
    with st.expander(f"{titles.get(tab, Details)} · {source} · {title}", expanded=True):

        # header
        top = f"**{title}**  \n`{ts}` · `{source}` · `{obj}` · color=`{color}` · score=`{score}` · T=`{th}` · label=`{label}`"
        st.markdown(top)

        if summary:
            st.markdown("**Summary**")
            st.write(summary)

        if rationale:
            st.markdown("**Rationale**")
            st.write(rationale)

        if impact_note:
            st.markdown("**Impact note**")
            st.write(impact_note)

        st.markdown("---")

        if tab == "impulse":
            # raw detail tabs
            t1, t2, t3, t4 = st.tabs(["Тело", "RAW", "META", "Related"])
            with t1:
                if body is not None and str(body).strip():
                    st.write(body if isinstance(body, str) else str(body))
                else:
                    txt = str(_get("text",""))
                    st.write(txt if txt else "—")
            with t2:
                if raw is None or (isinstance(raw, str) and not raw.strip()):
                    st.write("—")
                else:
                    st.code(raw if isinstance(raw, str) else str(raw), language="json")
            with t3:
                if meta is None or (isinstance(meta, str) and not meta.strip()):
                    st.write("—")
                else:
                    st.code(meta if isinstance(meta, str) else str(meta), language="json")
            with t4:
                try:
                    d = df_ctx.copy()
                    # нормализуем ts
                    if "ts" in d.columns:
                        d["_ts"] = pd.to_datetime(d["ts"], errors="coerce", utc=True)
                    # related by object+source (fallback: source only)
                    if obj and "object" in d.columns:
                        rel = d[(d["source"].astype(str) == source) & (d["object"].astype(str) == obj)]
                    else:
                        rel = d[d["source"].astype(str) == source]
                    rel = rel.sort_values("_ts", ascending=False).head(10)
                    cols = [c for c in ["ts","source","object","color","score","label","title","url"] if c in rel.columns]
                    st.dataframe(rel[cols], width="stretch", height=320)
                except Exception as e:
                    st.write("related error:", e)

        elif tab == "analytics":
            try:
                d = df_ctx.copy()
                if "ts" in d.columns:
                    d["_ts"] = pd.to_datetime(d["ts"], errors="coerce", utc=True)
                if obj and "object" in d.columns:
                    d = d[(d["source"].astype(str) == source) & (d["object"].astype(str) == obj)]
                else:
                    d = d[d["source"].astype(str) == source]

                d = d.dropna(subset=["_ts"]).sort_values("_ts")
                st.caption("Score over time (по текущему контексту/фильтрам)")
                if "score" in d.columns and not d.empty:
                    st.line_chart(d.set_index("_ts")["score"])
                else:
                    st.info("Недостаточно данных для графика score.")

                st.caption("Colors distribution")
                if "color" in d.columns and not d.empty:
                    vc = d["color"].astype(str).value_counts()
                    st.bar_chart(vc)
            except Exception as e:
                st.write("analytics error:", e)

        elif tab == "decision":
            # deterministic / rule-based decision
            try:
                sc = float(score) if score is not None and str(score) != "" else None
            except Exception:
                sc = None

            # base thresholds (can tune later)
            if color == "🔴":
                level = "T0"
                act = [
                    "Проверить первоисточник (что изменилось).",
                    "Сравнить с предыдущими сигналами по тому же object/source.",
                    "Если подтверждается — усилить сбор/частоту (автоматически, без ручного ввода).",
                ]
            elif color == "🟡":
                level = "T1"
                act = [
                    "Наблюдать. Дождаться ещё 1–2 подтверждающих сигналов.",
                    "Включить/оставить авто-рефреш, не дергать вручную.",
                ]
            else:
                # fallback by score
                if sc is not None and sc >= 0.58:
                    level = "T1"
                    act = ["Порог высокий: держать в фокусе, ждать подтверждения/контекста."]
                else:
                    level = "T2"
                    act = ["Фон. Действий не требуется."]

            st.markdown(f"### {level} · Решение")
            for i, a in enumerate(act, 1):
                st.markdown(f"{i}. {a}")

            if th:
                st.markdown(f"**T-horizon:** `{th}`")
            st.markdown("---")
            st.caption("Это детерминированное решение уровня A. Позже можно усилить правилами/метриками, без LLM.")

        # close control
        if st.button("Закрыть панель", key=f"close_{sid}"):
            st.session_state["pa_open"] = {"sid": None, "tab": None}
            st.rerun()


def planet_nav():
    st.markdown(
        """
<div class="pa-planets">
  <a class="pa-planet" href="#sec-priority"><small>1</small></a>
  <a class="pa-planet" href="#sec-all"><small>2</small></a>
  <a class="pa-planet" href="#sec-analytics"><small>3</small></a>
  <a class="pa-planet" href="#sec-about"><small>4</small></a>
</div>
""",
        unsafe_allow_html=True,
    )

def main():
    st.markdown('<div class="pa-title">Pulse Atlas</div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-sub">Local signal monitor · core objects · one-page dashboard</div>', unsafe_allow_html=True)

    # Sidebar filters
    with st.sidebar:
        st.markdown("### Filters")
        st.caption("Scroll page + planet nav (anchors). Без JS — не ломается.")
        st.markdown("---")

        df_all = load_signals(DB_PATH)

        df_actions = load_actions(DB_PATH)
        # choices
        colors = ["All"]
        if not df_all.empty and "color" in df_all.columns:
            colors += sorted([c for c in df_all["color"].dropna().unique().tolist() if str(c).strip()])

        sources = ["All"]
        if not df_all.empty and "source" in df_all.columns:
            sources += sorted([s for s in df_all["source"].dropna().unique().tolist() if str(s).strip()])

        color = st.selectbox("Color", colors, index=0)
        source = st.selectbox("Source", sources, index=0)

        # score range
        if not df_all.empty and "score" in df_all.columns:
            sc = pd.to_numeric(df_all["score"], errors="coerce").dropna()
            if not sc.empty:
                mn, mx = float(sc.min()), float(sc.max())
            else:
                mn, mx = 0.0, 1.0
        else:
            mn, mx = 0.0, 1.0
        score_min, score_max = st.slider("Score range", min_value=float(mn), max_value=float(mx), value=(float(mn), float(mx)))

        period = st.selectbox("Period", ["7d", "30d", "90d", "365d"], index=1)
        days = int(period.replace("d", ""))

        if st.button("Refresh Data"):
            st.cache_data.clear()

        st.caption(f"Auto-refresh: {REFRESH_SEC}s")

    # Data load + filter
    df_all = load_signals(DB_PATH)
    df_filtered = filter_df(df_all, color, source, score_min, score_max, days)

    # Top metrics + nav
    metrics_block(df_all, df_filtered)
    planet_nav()

    # Sections (scroll)
    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div id="sec-priority" class="pa-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-section">', unsafe_allow_html=True)
    priority_view(df_filtered)
    st.markdown("<div class=\"hr\"></div>", unsafe_allow_html=True)
    st.markdown("<div id=\"sec-actions\" class=\"pa-anchor\"></div>", unsafe_allow_html=True)
    st.markdown("<div class=\"pa-section\">", unsafe_allow_html=True)
        actions_view(df_actions)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div id="sec-all" class="pa-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-section">', unsafe_allow_html=True)
    table_view(df_filtered, "All Signals")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div id="sec-analytics" class="pa-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-section">', unsafe_allow_html=True)
    analytics_view(df_filtered)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div id="sec-about" class="pa-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="pa-section">', unsafe_allow_html=True)
    about_view(DB_PATH, df_all)
    st.markdown('</div>', unsafe_allow_html=True)

    # Auto refresh
    st.caption("")
    st.caption("Tip: планеты справа — кликабельные якоря. Это стабильная основа под будущий космо-интерфейс.")
if __name__ == "__main__":
    main()
