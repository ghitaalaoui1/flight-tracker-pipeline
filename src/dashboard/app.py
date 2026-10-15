import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pydeck as pdk
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")

# Bounding box used by the producer, so the map always frames Morocco
MAP_CENTER = {"lat": 31.5, "lon": -7.0, "zoom": 4.5}

st.set_page_config(page_title="Flight Tracker", layout="wide")
st.title("Air traffic over Morocco")


@st.cache_resource
def get_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client["flights"]["positions"]


def load_recent(minutes=15):
    since = int((datetime.now(timezone.utc) - timedelta(minutes=minutes)).timestamp())
    cursor = get_collection().find({"timestamp": {"$gte": since}})
    return pd.DataFrame(list(cursor))


try:
    df = load_recent()
except PyMongoError:
    st.error(
        "Cannot reach MongoDB. Check that the stack is running "
        "(`docker compose ps`) and that the database is listening on "
        f"{MONGO_URI}."
    )
    st.stop()

if df.empty:
    st.warning("No recent data. Is the pipeline running?")
    st.stop()

# Keep one row per aircraft: its most recent position
latest = df.sort_values("timestamp").groupby("icao24").last().reset_index()

col1, col2, col3 = st.columns(3)
col1.metric("Aircraft tracked", len(latest))
col2.metric("Countries", latest["country"].nunique())
col3.metric("Average altitude", f"{latest['altitude_m'].mean():.0f} m")

st.subheader("Positions")

map_data = latest[["latitude", "longitude", "callsign", "country", "altitude_m"]].dropna(
    subset=["latitude", "longitude"]
)

st.pydeck_chart(
    pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=MAP_CENTER["lat"],
            longitude=MAP_CENTER["lon"],
            zoom=MAP_CENTER["zoom"],
            pitch=0,
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position="[longitude, latitude]",
                get_radius=8000,
                get_fill_color=[200, 30, 0, 160],
                pickable=True,
            )
        ],
        tooltip={"text": "{callsign}\n{country}\n{altitude_m} m"},
    )
)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("By country")
    st.bar_chart(latest["country"].value_counts().head(10))

with col_right:
    st.subheader("On approach or departure")
    approach = latest[
        (latest["altitude_m"] < 1500)
        & (latest["speed_kmh"] < 400)
        & (~latest["on_ground"])
    ]
    if approach.empty:
        st.info("No aircraft on approach right now.")
    else:
        st.dataframe(
            approach[["callsign", "country", "altitude_m", "speed_kmh"]],
            hide_index=True,
        )

st.subheader("All flights")
st.dataframe(
    latest[["callsign", "country", "altitude_m", "speed_kmh", "on_ground"]].sort_values(
        "altitude_m", ascending=False
    ),
    hide_index=True,
)

if st.button("Refresh"):
    st.rerun()