import os
import time
import uuid
import sqlite3
import streamlit as st
import pandas as pd

DB_PATH = "mock_cache_data.db"

st.title("cache_data / cache_resource の可視化デモ")

# -------- cache_resource: DB接続(=リソース)を作って使い回す --------
@st.cache_resource
def get_conn_and_meta():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    meta = {
        "resource_uuid": str(uuid.uuid4()),  # リソース作成時に1回だけ決まる
        "resource_created_at_unix": time.time(),
        "conn_id": id(conn),
    }
    return conn, meta


# -------- cache_data: SELECT結果(=データ)をキャッシュ --------
@st.cache_data(ttl=60)  # 例: 60秒で更新追従（3分更新なら60〜180秒が目安）
def load_recent_df(limit: int, db_mtime: float):
    # ※db_mtimeを引数に入れるとDBファイル更新でキャッシュが自動無効化される
    conn, _ = get_conn_and_meta()
    df = pd.read_sql_query(
        "SELECT * FROM chuoh_temp ORDER BY ts DESC LIMIT ?",
        conn,
        params=(limit,),
    )
    # 「このデータがいつ作られたか」を見せる用
    df.attrs["data_generated_at_unix"] = time.time()
    return df


# UI
limit = st.slider("表示件数", 10, 500, 100, step=10)

# DB更新検知用（ファイルが更新されると mtime が変わる）
db_mtime = os.path.getmtime(DB_PATH)

conn, rmeta = get_conn_and_meta()
df = load_recent_df(limit, db_mtime=db_mtime)

# -------- 出力（resource / data を両方表示）--------
st.subheader("cache_resource（リソース）側の情報")
st.write(
    {
        "resource_uuid (同一プロセス内で固定になりやすい)": rmeta["resource_uuid"],
        "resource_created_at_unix": rmeta["resource_created_at_unix"],
        "conn_id": rmeta["conn_id"],
        "db_path": DB_PATH,
    }
)

st.subheader("cache_data（データ）側の情報")
st.write(
    {
        "data_generated_at_unix (ttl/mtimeで更新される)": df.attrs.get("data_generated_at_unix"),
        "rows": len(df),
        "db_mtime_used_as_cache_key": db_mtime,
    }
)

st.subheader("chuoh_temp 最新データ")
st.dataframe(df, use_container_width=True)

st.caption("ヒント: ブラウザでリロードしても resource_uuid が変わりにくく、data_generated_at_unix は ttl/DB更新で変わるはずです。")
