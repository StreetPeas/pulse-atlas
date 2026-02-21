import sqlite3
from pathlib import Path
from urllib.parse import quote_plus
import pandas as pd
import streamlit as st

# Ensure DB schema exists
try:
    import storage
    storage.init_db()
except Exception as e:
    print(f"[warn] init_db failed: {e}")

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make df.columns unique: a, a -> a, a__2 (stable)."""
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        c0 = str(c)
        n = seen.get(c0, 0) + 1
        seen[c0] = n
        new_cols.append(c0 if n == 1 else f"{c0}__{n}")
    df = df.copy()
    df.columns = new_cols
    return df

def safe_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """PyArrow/Streamlit safe: unique cols + stringified object columns."""
    df = dedupe_columns(df)
    df = df.copy()
    # make object columns safer for arrow
    for c in df.columns:
        try:
            if df[c].dtype == "object":
                df[c] = df[c].map(lambda v: "" if v is None else str(v))
        except Exception:
            pass
    return df

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make df.columns unique: a, a -> a, a__2 (stable)."""
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        c0 = str(c)
        n = seen.get(c0, 0) + 1
        seen[c0] = n
        new_cols.append(c0 if n == 1 else f"{c0}__{n}")
    df = df.copy()
    df.columns = new_cols
    return df

DB_PATH = Path("data/atlas.db")

# --------------------------
# DB
# --------------------------
def _connect():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

def _table_exists(conn, name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cur.fetchone() is not None

def load_signals(limit: int = 5000) -> pd.DataFrame:
    """Best-effort loader: tries common tables, returns DataFrame (possibly empty)."""
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = _connect()
    try:
        # предпочитаем signals, иначе entries (или что найдём)
        table = None
        for cand in ("signals", "entries", "journal", "logs"):
            if _table_exists(conn, cand):
                table = cand
                break

        if table is None:
            return pd.DataFrame()

        df = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT ?", conn, params=(limit,))
        return df
    finally:
        conn.close()

# --------------------------
# UI helpers
# --------------------------
def g_url(query: str) -> str:
    q = quote_plus((query or "").strip())
    return f"https://www.google.com/search?q={q}"

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Try to standardize common column names for display."""
    if df.empty:
        return df

    # возможные имена
    ren = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("ts", "timestamp", "created_at", "time", "datetime"):
            ren[c] = "ts"
        elif lc in ("kind", "type", "signal_type", "category"):
            ren[c] = "type"
        elif lc in ("text", "content", "body", "message", "title"):
            ren[c] = "text"
        elif lc in ("source", "src"):
            ren[c] = "source"
        elif lc in ("id", "sid", "uuid"):
            ren[c] = "id"
    df = df.rename(columns=ren)
    return df

def _as_text(v) -> str:
    try:
        if v is None:
            return ""
        # pandas NA/NaT or numpy nan
        try:
            import pandas as _pd
            # _pd.isna on non-scalar returns array/Series -> avoid boolean context
            r = _pd.isna(v)
            if isinstance(r, (bool,)):
                if r:
                    return ""
        except Exception:
            pass
        t = str(v).strip()
        return "" if t in ("", "nan", "NaT", "None") else t
    except Exception:
        return ""

def _as_text(v) -> str:
    """Safe stringify for scalars / lists / dicts / pandas objects."""
    try:
        if v is None:
            return ""
        try:
            import pandas as _pd
            r = _pd.isna(v)
            if isinstance(r, bool) and r:
                return ""
        except Exception:
            pass
        t = str(v).strip()
        return "" if t in ("", "nan", "NaT", "None") else t
    except Exception:
        return ""

def pick_text(row: pd.Series) -> str:
    for k in ("text", "content", "message", "title", "summary"):
        if k in row.index:
            t = _as_text(row.get(k))
            if t:
                return t

    # fallback: первые поля как строка
    parts = []
    for k in list(row.index)[:6]:
        parts.append(f"{k}={_as_text(row.get(k))}")
    return " | ".join(parts)

def safe_id(row: pd.Series, fallback: int) -> str:
    for k in ("id", "sid", "uuid"):
        if k in row.index and str(row.get(k, "")).strip() not in ("", "nan", "NaT", "None"):
            return str(row[k])
    return str(fallback)

# --------------------------
# Views
# --------------------------
def sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Фильтры")

    if df.empty:
        st.sidebar.info("База пуста или таблица не найдена.")
        return df

    # тип
    if "type" in df.columns:
        types = sorted([t for t in df["type"].dropna().astype(str).unique()])
        sel = st.sidebar.multiselect("Type", types, default=types)
        if sel:
            df = df[df["type"].astype(str).isin(sel)]

    # источник
    if "source" in df.columns:
        sources = sorted([s for s in df["source"].dropna().astype(str).unique()])
        sel_s = st.sidebar.multiselect("Source", sources, default=sources)
        if sel_s:
            df = df[df["source"].astype(str).isin(sel_s)]

    # поиск
    q = st.sidebar.text_input("Поиск по тексту", value="")
    if q.strip():
        qq = q.strip().lower()
        df = df[df.apply(lambda r: qq in pick_text(r).lower(), axis=1)]

    return df

def table_view(df: pd.DataFrame):
    st.subheader("Сигналы")
    df = dedupe_columns(df)
    st.dataframe(safe_df_for_display(df), width="stretch", height=420)

def cards_view(df: pd.DataFrame):
    st.subheader("Карточки")

    if df.empty:
        st.info("Нет данных для отображения.")
        return

    # сколько карточек
    n = st.slider("Количество карточек", 5, 200, 25, step=5, key="n_cards")
    df2 = df.head(n)

    for i, (_, row) in enumerate(df2.iterrows(), 1):
        sid = safe_id(row, i)
        text = pick_text(row)

        with st.container(border=True):
            cols = st.columns([6, 2, 2, 2])
            cols[0].markdown(f"**{text[:200]}**")
            if "ts" in row.index and pd.notna(row["ts"]):
                cols[0].caption(str(row["ts"]))

            # 1) Импульс (локальная отметка, ничего в БД не пишет)
            if cols[1].button("Импульс", key=f"imp_{sid}_{i}"):
                st.session_state["selected_signal"] = sid
                st.toast(f"Импульс: {sid}", icon="✅")

            # 2) Аналитика/Решение = Google (как ты просил)
            q_ana = f"analyze {text[:120]}"
            q_sol = f"how to fix {text[:120]}"

            cols[2].markdown(f"[Аналитика]({g_url(q_ana)})", unsafe_allow_html=True)
            cols[3].markdown(f"[Решение]({g_url(q_sol)})", unsafe_allow_html=True)

def actions_view(df: pd.DataFrame):
    st.subheader("Импульс")
    sid = st.session_state.get("selected_signal")
    if not sid:
        st.info("Нажми «Импульс» на любой карточке.")
        return
    st.success(f"Выбран сигнал: {sid}")
    st.caption("Это только UI-выбор. В БД ничего не записывается.")

# --------------------------
# Main
# --------------------------
def main():
    st.set_page_config(page_title="Pulse Atlas", layout="wide")

    st.title("Pulse Atlas · Dashboard (one-file)")

    df = load_signals(limit=5000)
    df = normalize_columns(df)

    df_f = sidebar_filters(df)

    df_f = dedupe_columns(df_f)
    tabs = st.tabs(["Карточки", "Таблица", "Импульс"])
    with tabs[0]:
        cards_view(df_f)
    with tabs[1]:
        table_view(df_f)
    with tabs[2]:
        actions_view(df_f)

if __name__ == "__main__":
    main()
