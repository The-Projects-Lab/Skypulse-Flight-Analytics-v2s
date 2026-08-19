import os
import sys
import glob
import re
import requests
import pandas as pd
import streamlit as st

from datetime import date
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="SkyPulse | Aviation Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# LOAD ENVIRONMENT
# ============================================================

load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HISTORICAL_PATH = os.path.join(
    BASE_DIR,
    "data/raw/historical_flight_prices.csv"
)

MODEL_PATH = os.path.join(
    BASE_DIR,
    "models/flight_price_model"
)

ROUTE_GOLD_PATH = os.path.join(
    BASE_DIR,
    "data/gold/route_price_analytics"
)

AIRLINE_GOLD_PATH = os.path.join(
    BASE_DIR,
    "data/gold/airline_price_analytics"
)

WINDOW_GOLD_PATH = os.path.join(
    BASE_DIR,
    "data/gold/price_window_analytics"
)

SILVER_PATH = os.path.join(
    BASE_DIR,
    "data/silver/live_flight_prices"
)


# ============================================================
# ROUTE / AIRPORT CONFIGURATION
# ============================================================

AIRPORTS = {
    "Delhi (DEL)": "DEL",
    "Mumbai (BOM)": "BOM",
    "Bangalore (BLR)": "BLR",
    "Chennai (MAA)": "MAA",
    "Kolkata (CCU)": "CCU",
    "Hyderabad (HYD)": "HYD"
}


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown("""
<style>

.main {
    background-color: #f7f9fc;
}

.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
}

[data-testid="stMetric"] {
    background-color: white;
    border: 1px solid #e5e7eb;
    padding: 18px;
    border-radius: 10px;
}

.section-title {
    font-size: 22px;
    font-weight: 700;
    color: #1f2937;
    margin-top: 15px;
    margin-bottom: 5px;
}

.section-subtitle {
    color: #6b7280;
    font-size: 14px;
    margin-bottom: 20px;
}

.live-box {
    background-color: #ecfdf5;
    border-left: 5px solid #10b981;
    padding: 15px;
    border-radius: 8px;
}

.prediction-box {
    background-color: #eff6ff;
    border-left: 5px solid #2563eb;
    padding: 15px;
    border-radius: 8px;
}

.warning-box {
    background-color: #fffbeb;
    border-left: 5px solid #f59e0b;
    padding: 15px;
    border-radius: 8px;
}

</style>
""", unsafe_allow_html=True)


# ============================================================
# SPARK SESSION
# ============================================================

@st.cache_resource
def get_spark():

    try:

        from src.utils.spark_session import create_spark_session

        spark = create_spark_session(
            "SkyPulse-Streamlit"
        )

        return spark

    except Exception as e:

        st.error(
            f"Spark session could not be created: {e}"
        )

        return None


# ============================================================
# LOAD HISTORICAL DATA
# ============================================================

@st.cache_data
def load_historical_data():

    if not os.path.exists(HISTORICAL_PATH):
        return pd.DataFrame()

    try:

        df = pd.read_csv(
            HISTORICAL_PATH
        )

        return df

    except Exception as e:

        st.error(
            f"Could not load historical data: {e}"
        )

        return pd.DataFrame()


# ============================================================
# LOAD DELTA DATA
# ============================================================

@st.cache_data(ttl=10)
def load_delta_data(path):

    spark = get_spark()

    if spark is None:
        return pd.DataFrame()

    try:

        df = (
            spark.read
            .format("delta")
            .load(path)
        )

        return df.toPandas()

    except Exception:
        return pd.DataFrame()


# ============================================================
# LOAD SPARK ML MODEL
# ============================================================

@st.cache_resource
def load_model():

    spark = get_spark()

    if spark is None:
        return None

    try:

        from pyspark.ml import PipelineModel

        model = PipelineModel.load(
            MODEL_PATH
        )

        return model

    except Exception as e:

        return None


# ============================================================
# PREDICT PRICE
# ============================================================

def predict_price(
    airline,
    source_city,
    destination_city,
    departure_time,
    arrival_time,
    cabin_class,
    duration_minutes,
    stops,
    days_left
):

    spark = get_spark()
    model = load_model()

    if spark is None or model is None:
        return None

    try:

        input_data = [
            (
                airline,
                source_city,
                destination_city,
                departure_time,
                arrival_time,
                cabin_class,
                float(duration_minutes),
                int(stops),
                int(days_left)
            )
        ]

        columns = [
            "airline",
            "source_city",
            "destination_city",
            "departure_time",
            "arrival_time",
            "cabin_class",
            "duration_minutes",
            "stops",
            "days_left"
        ]

        input_df = spark.createDataFrame(
            input_data,
            columns
        )

        prediction_df = model.transform(
            input_df
        )

        prediction = (
            prediction_df
            .select("prediction")
            .first()[0]
        )

        return float(prediction)

    except Exception as e:

        st.error(
            f"Prediction failed: {e}"
        )

        return None


# ============================================================
# HELPER - TIME STRING
# ============================================================

def clean_time(value):

    if value is None:
        return "00:00"

    value = str(value).strip()

    time_matches = re.findall(
        r"\b\d{1,2}:\d{2}\b",
        value
    )

    if time_matches:
        return time_matches[-1]

    return value


# ============================================================
# HELPER - DURATION
# ============================================================

def parse_duration_minutes(duration):

    if duration is None:
        return 0

    duration = str(duration).lower()

    hours = 0
    minutes = 0

    try:

        if "h" in duration:

            hours_part = duration.split("h")[0]

            hours = int(
                "".join(
                    c for c in hours_part
                    if c.isdigit()
                )
            )

        if "m" in duration:

            if "h" in duration:

                minutes_part = (
                    duration
                    .split("h")[1]
                    .split("m")[0]
                )

            else:

                minutes_part = (
                    duration
                    .split("m")[0]
                )

            digits = "".join(
                c for c in minutes_part
                if c.isdigit()
            )

            if digits:
                minutes = int(digits)

    except Exception:
        return 0

    return hours * 60 + minutes


# ============================================================
# HELPER - SERPAPI PRICE
# ============================================================

def parse_live_price(value):

    if value is None:
        return None

    if isinstance(value, (int, float)):
        if value > 0:
            return float(value)

        return None

    value = str(value)

    digits = re.sub(
        r"[^0-9.]",
        "",
        value
    )

    if not digits:
        return None

    try:
        price = float(digits)

    except ValueError:
        return None

    if price <= 0:
        return None

    return price


def extract_flight_group_price(flight_group):

    for key in [
        "price",
        "extracted_price",
        "total_price"
    ]:

        price = parse_live_price(
            flight_group.get(key)
        )

        if price is not None:
            return price

    return None


def extract_duration_minutes(flight_group, flight_list):

    duration = flight_group.get(
        "total_duration"
    )

    if isinstance(duration, str):
        return parse_duration_minutes(duration)

    if duration:
        return int(duration)

    leg_minutes = []

    for flight in flight_list:

        leg_duration = flight.get("duration")

        if isinstance(leg_duration, str):
            leg_duration = parse_duration_minutes(
                leg_duration
            )

        if leg_duration:
            leg_minutes.append(
                int(leg_duration)
            )

    return sum(leg_minutes)


# ============================================================
# SERPAPI GOOGLE FLIGHTS
# ============================================================

def search_live_flights(
    departure_id,
    arrival_id,
    travel_date,
    travel_class
):

    if not SERPAPI_KEY:

        return None, (
            "SERPAPI_KEY not found. "
            "Check your .env file."
        )

    travel_class_map = {
        "Economy": 1,
        "Premium Economy": 2,
        "Business": 3,
        "First": 4
    }

    params = {
        "engine": "google_flights",
        "departure_id": departure_id,
        "arrival_id": arrival_id,
        "outbound_date": str(travel_date),
        "currency": "INR",
        "hl": "en",
        "gl": "in",
        "type": 2,
        "travel_class": travel_class_map[
            travel_class
        ],
        "api_key": SERPAPI_KEY
    }

    try:

        response = requests.get(
            "https://serpapi.com/search",
            params=params,
            timeout=60
        )

        response.raise_for_status()

        data = response.json()

        if "error" in data:

            return None, data["error"]

        flights = []

        best_flights = data.get(
            "best_flights",
            []
        )

        other_flights = data.get(
            "other_flights",
            []
        )

        all_flights = (
            best_flights +
            other_flights
        )

        for flight_group in all_flights:

            price = extract_flight_group_price(
                flight_group
            )

            flight_list = flight_group.get(
                "flights",
                []
            )

            if not flight_list:
                continue

            first_flight = flight_list[0]
            last_flight = flight_list[-1]

            airline = first_flight.get(
                "airline",
                "Unknown"
            )

            departure_airport = (
                first_flight
                .get("departure_airport", {})
            )

            arrival_airport = (
                last_flight
                .get("arrival_airport", {})
            )

            departure_time = (
                departure_airport.get(
                    "time",
                    "00:00"
                )
            )

            arrival_time = (
                arrival_airport.get(
                    "time",
                    "00:00"
                )
            )

            duration_minutes = extract_duration_minutes(
                flight_group,
                flight_list
            )

            stops = max(
                len(flight_list) - 1,
                0
            )

            flights.append({
                "airline": airline,
                "departure_time": clean_time(
                    departure_time
                ),
                "arrival_time": clean_time(
                    arrival_time
                ),
                "duration_minutes": duration_minutes,
                "stops": stops,
                "live_price": price,
                "currency": "INR"
            })

        if not flights:

            response_keys = ", ".join(
                data.keys()
            )

            return None, (
                "No flights found for this search. "
                f"SerpAPI sections: best_flights="
                f"{len(best_flights)}, other_flights="
                f"{len(other_flights)}. Response keys: "
                f"{response_keys}"
            )

        return pd.DataFrame(flights), None

    except Exception as e:

        return None, str(e)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title("✈️ SkyPulse")

    st.caption(
        "Real-Time Aviation Fare Analytics"
    )

    st.divider()

    page = st.radio(
        "Navigation",
        [
            "Dashboard",
            "Live vs Predicted",
            "Historical Analytics",
            "Gold Analytics",
            "Live Pipeline"
        ]
    )

    st.divider()

    st.caption("Technology Stack")

    st.write(
        "PySpark • Kafka • Spark SQL"
    )

    st.write(
        "Delta Lake • ML • SerpAPI"
    )


# Refresh only pages that display continuously changing
# pipeline data. Search pages must not rerun mid-request.
if page in [
    "Dashboard",
    "Live Pipeline"
]:

    st_autorefresh(
        interval=10000,
        key="skypulse_live_refresh"
    )


# ============================================================
# LOAD CORE DATA
# ============================================================

historical_df = load_historical_data()


# ============================================================
# HEADER
# ============================================================

st.title("✈️ SkyPulse Aviation Analytics")

st.caption(
    "Historical + Streaming Flight Data | "
    "Live Google Flights vs ML Price Prediction"
)


# ============================================================
# PAGE 1 - DASHBOARD
# ============================================================

if page == "Dashboard":

    st.markdown(
        '<div class="section-title">'
        'Platform Overview'
        '</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-subtitle">'
        'End-to-end aviation fare analytics pipeline'
        '</div>',
        unsafe_allow_html=True
    )

    live_silver_df = load_delta_data(
        SILVER_PATH
    )

    route_gold_df = load_delta_data(
        ROUTE_GOLD_PATH
    )

    airline_gold_df = load_delta_data(
        AIRLINE_GOLD_PATH
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.metric(
            "Historical Records",
            f"{len(historical_df):,}"
        )

    with col2:

        st.metric(
            "Live Silver Records",
            f"{len(live_silver_df):,}"
        )

    with col3:

        if not route_gold_df.empty:
            routes = route_gold_df[
                "route"
            ].nunique()
        else:
            routes = 0

        st.metric(
            "Routes Analyzed",
            routes
        )

    with col4:

        st.metric(
            "ML Model",
            "Spark Pipeline"
        )

    st.divider()

    left, right = st.columns([2, 1])

    with left:

        st.subheader(
            "Historical Flight Data"
        )

        if historical_df.empty:

            st.warning(
                "Historical dataset not found."
            )

        else:

            st.success(
                f"Loaded {len(historical_df):,} "
                "historical flight records"
            )

            st.dataframe(
                historical_df.head(20),
                use_container_width=True
            )

    with right:

        st.subheader(
            "Pipeline Architecture"
        )

        st.code(
            """
Historical CSV
      ↓
Bronze Layer
      ↓
Silver Layer
      ↓
Gold Analytics
      ↓
Spark ML Model

SerpAPI → Kafka → Bronze
               ↓
             Silver
               ↓
          Gold Analytics
               ↓
       Live vs Prediction
            """
        )


# ============================================================
# PAGE 2 - LIVE VS PREDICTED
# ============================================================

elif page == "Live vs Predicted":

    st.markdown(
        '<div class="section-title">'
        'Live Fare vs ML Predicted Fare'
        '</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Search Google Flights using SerpAPI and compare '
        'the live market fare against the Random Forest '
        'price prediction model.'
        '</div>',
        unsafe_allow_html=True
    )

    c1, c2 = st.columns(2)

    with c1:

        source = st.selectbox(
            "From",
            list(AIRPORTS.keys()),
            index=0
        )

    with c2:

        destination_options = [
            x for x in AIRPORTS.keys()
            if x != source
        ]

        destination = st.selectbox(
            "To",
            destination_options
        )

    c3, c4 = st.columns(2)

    with c3:

        travel_date = st.date_input(
            "Travel Date",
            value=date.today()
        )

    with c4:

        cabin_class = st.selectbox(
            "Class",
            [
                "Economy",
                "Premium Economy",
                "Business",
                "First"
            ]
        )

    source_code = AIRPORTS[source]
    destination_code = AIRPORTS[destination]

    source_city = source.split(" (")[0]
    destination_city = destination.split(" (")[0]

    if st.button(
        "🔍 Search Live Flights & Compare",
        type="primary",
        use_container_width=True
    ):

        with st.spinner(
            "Calling SerpAPI and searching Google Flights..."
        ):

            live_df, error = search_live_flights(
                source_code,
                destination_code,
                travel_date,
                cabin_class
            )

        if error:

            st.error(
                f"Live search failed: {error}"
            )

        else:

            days_left = (
                travel_date - date.today()
            ).days

            days_left = max(
                days_left,
                0
            )

            st.success(
                f"Found {len(live_df)} live flight options"
            )

            predicted_prices = []

            progress = st.progress(0)

            for index, row in live_df.iterrows():

                prediction = predict_price(
                    airline=row["airline"],
                    source_city=source_city,
                    destination_city=destination_city,
                    departure_time=row[
                        "departure_time"
                    ],
                    arrival_time=row[
                        "arrival_time"
                    ],
                    cabin_class=cabin_class,
                    duration_minutes=row[
                        "duration_minutes"
                    ],
                    stops=row["stops"],
                    days_left=days_left
                )

                predicted_prices.append(
                    prediction
                )

                progress.progress(
                    int(
                        ((index + 1) /
                         len(live_df)) * 100
                    )
                )

            live_df[
                "predicted_price"
            ] = predicted_prices

            live_df["difference"] = (
                live_df["live_price"] -
                live_df["predicted_price"]
            )

            live_df[
                "difference_percent"
            ] = (
                live_df["difference"] /
                live_df["predicted_price"] *
                100
            )

            progress.empty()

            # Remove prediction failures
            comparison_df = live_df.dropna(
                subset=[
                    "live_price",
                    "predicted_price"
                ]
            ).copy()

            if comparison_df.empty:

                if live_df["live_price"].dropna().empty:

                    st.error(
                        "SerpAPI returned flight itineraries, "
                        "but no usable fares were found for "
                        "comparison."
                    )

                else:

                    st.error(
                        "Live flights were found but the "
                        "ML model could not generate predictions."
                    )

                st.subheader(
                    "Live Search Results"
                )

                st.dataframe(
                    live_df,
                    use_container_width=True
                )

            else:

                comparison_df = comparison_df.sort_values(
                    "live_price"
                )

                best = comparison_df.iloc[0]

                col1, col2, col3, col4 = st.columns(4)

                col1.metric(
                    "Lowest Live Fare",
                    f"₹{best['live_price']:,.0f}"
                )

                col2.metric(
                    "ML Predicted Fare",
                    f"₹{best['predicted_price']:,.0f}"
                )

                difference = best["difference"]

                col3.metric(
                    "Price Difference",
                    f"₹{difference:,.0f}"
                )

                signal = "Stable"

                if difference > 1000:
                    signal = "Higher than Prediction ⚠️"

                elif difference < -1000:
                    signal = "Below Prediction 🟢"

                col4.metric(
                    "Price Signal",
                    signal
                )

                st.divider()

                st.subheader(
                    "Flight-by-Flight Comparison"
                )

                display_columns = [
                    "airline",
                    "departure_time",
                    "arrival_time",
                    "duration_minutes",
                    "stops",
                    "live_price",
                    "predicted_price",
                    "difference",
                    "difference_percent"
                ]

                comparison_df[
                    display_columns
                ] = comparison_df[
                    display_columns
                ].round(2)

                st.dataframe(
                    comparison_df[
                        display_columns
                    ],
                    use_container_width=True
                )

                st.subheader(
                    "Live Price vs Predicted Price"
                )

                chart_df = comparison_df[
                    [
                        "live_price",
                        "predicted_price"
                    ]
                ]

                st.bar_chart(chart_df)


# ============================================================
# PAGE 3 - HISTORICAL ANALYTICS
# ============================================================

elif page == "Historical Analytics":

    st.markdown(
        '<div class="section-title">'
        'Historical Flight Fare Analytics'
        '</div>',
        unsafe_allow_html=True
    )

    if historical_df.empty:

        st.error(
            f"Historical data not found at: "
            f"{HISTORICAL_PATH}"
        )

    else:

        st.success(
            f"{len(historical_df):,} records loaded "
            "from the historical dataset."
        )

        st.divider()

        st.subheader(
            "Dataset Columns"
        )

        st.write(
            list(historical_df.columns)
        )

        # ----------------------------------------------------
        # PRICE ANALYTICS
        # ----------------------------------------------------

        price_column = None

        for column in [
            "price",
            "Price",
            "fare"
        ]:

            if column in historical_df.columns:
                price_column = column
                break

        if price_column:

            historical_df[price_column] = pd.to_numeric(
                historical_df[price_column],
                errors="coerce"
            )

            c1, c2, c3 = st.columns(3)

            c1.metric(
                "Average Historical Fare",
                f"₹{historical_df[price_column].mean():,.0f}"
            )

            c2.metric(
                "Minimum Fare",
                f"₹{historical_df[price_column].min():,.0f}"
            )

            c3.metric(
                "Maximum Fare",
                f"₹{historical_df[price_column].max():,.0f}"
            )

        st.divider()

        # ----------------------------------------------------
        # AIRLINE ANALYTICS
        # ----------------------------------------------------

        airline_column = None

        for column in [
            "airline",
            "Airline"
        ]:

            if column in historical_df.columns:
                airline_column = column
                break

        if airline_column:

            st.subheader(
                "Average Fare by Airline"
            )

            if price_column:

                airline_chart = (
                    historical_df
                    .groupby(airline_column)[
                        price_column
                    ]
                    .mean()
                    .sort_values(
                        ascending=False
                    )
                )

                st.bar_chart(
                    airline_chart
                )

        # ----------------------------------------------------
        # ROUTE ANALYTICS
        # ----------------------------------------------------

        source_column = None
        destination_column = None

        for column in [
            "source_city",
            "Source"
        ]:

            if column in historical_df.columns:
                source_column = column
                break

        for column in [
            "destination_city",
            "Destination"
        ]:

            if column in historical_df.columns:
                destination_column = column
                break

        if (
            source_column and
            destination_column and
            price_column
        ):

            st.subheader(
                "Average Historical Fare by Route"
            )

            temp_df = historical_df.copy()

            temp_df["route"] = (
                temp_df[source_column].astype(str)
                + " → " +
                temp_df[destination_column].astype(str)
            )

            route_chart = (
                temp_df
                .groupby("route")[price_column]
                .mean()
                .sort_values(
                    ascending=False
                )
                .head(15)
            )

            st.bar_chart(
                route_chart
            )

        st.divider()

        st.subheader(
            "Historical Data Sample"
        )

        st.dataframe(
            historical_df.head(100),
            use_container_width=True
        )


# ============================================================
# PAGE 4 - GOLD ANALYTICS
# ============================================================

elif page == "Gold Analytics":

    st.markdown(
        '<div class="section-title">'
        'Gold Layer Analytics'
        '</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Streaming analytics generated using Spark SQL '
        'and stored in Delta Lake.'
        '</div>',
        unsafe_allow_html=True
    )

    route_df = load_delta_data(
        ROUTE_GOLD_PATH
    )

    airline_df = load_delta_data(
        AIRLINE_GOLD_PATH
    )

    window_df = load_delta_data(
        WINDOW_GOLD_PATH
    )

    tab1, tab2, tab3 = st.tabs([
        "Route Price Analytics",
        "Airline Price Analytics",
        "Time Window Analytics"
    ])

    # --------------------------------------------------------
    # ROUTE ANALYTICS
    # --------------------------------------------------------

    with tab1:

        if route_df.empty:

            st.warning(
                "No route analytics available yet."
            )

        else:

            st.metric(
                "Route Analytics Records",
                len(route_df)
            )

            st.dataframe(
                route_df,
                use_container_width=True
            )

            if (
                "route" in route_df.columns and
                "avg_price" in route_df.columns
            ):

                route_chart = (
                    route_df
                    .groupby("route")[
                        "avg_price"
                    ]
                    .mean()
                    .sort_values(
                        ascending=False
                    )
                )

                st.bar_chart(
                    route_chart
                )

    # --------------------------------------------------------
    # AIRLINE ANALYTICS
    # --------------------------------------------------------

    with tab2:

        if airline_df.empty:

            st.warning(
                "No airline analytics available yet."
            )

        else:

            st.metric(
                "Airline Analytics Records",
                len(airline_df)
            )

            st.dataframe(
                airline_df,
                use_container_width=True
            )

            if (
                "airline" in airline_df.columns and
                "avg_price" in airline_df.columns
            ):

                airline_chart = (
                    airline_df
                    .groupby("airline")[
                        "avg_price"
                    ]
                    .mean()
                    .sort_values(
                        ascending=False
                    )
                )

                st.bar_chart(
                    airline_chart
                )

    # --------------------------------------------------------
    # WINDOW ANALYTICS
    # --------------------------------------------------------

    with tab3:

        if window_df.empty:

            st.warning(
                "No time window analytics available yet."
            )

        else:

            st.metric(
                "Window Analytics Records",
                len(window_df)
            )

            st.dataframe(
                window_df,
                use_container_width=True
            )

            if (
                "window_start" in window_df.columns and
                "avg_price" in window_df.columns
            ):

                chart_df = window_df.copy()

                chart_df[
                    "window_start"
                ] = pd.to_datetime(
                    chart_df["window_start"]
                )

                chart_df = chart_df.sort_values(
                    "window_start"
                )

                st.line_chart(
                    chart_df.set_index(
                        "window_start"
                    )["avg_price"]
                )


# ============================================================
# PAGE 5 - LIVE PIPELINE
# ============================================================

elif page == "Live Pipeline":

    st.markdown(
        '<div class="section-title">'
        'Kafka Live Flight Feed'
        '</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Live events flowing through '
        'Kafka → Spark Structured Streaming → '
        'Silver Delta Lake → Gold Spark SQL Analytics'
        '</div>',
        unsafe_allow_html=True
    )

    # --------------------------------------------------------
    # LOAD FRESH SILVER DATA
    # --------------------------------------------------------

    live_df = load_delta_data(SILVER_PATH)

    if live_df.empty:

        st.warning(
            "No Silver streaming records found yet."
        )

        st.info(
            "Start Zookeeper, Kafka, Spark Kafka Consumer "
            "and Kafka Producer."
        )

    else:

        # ----------------------------------------------------
        # CLEAN TIMESTAMP
        # ----------------------------------------------------

        if "event_timestamp" in live_df.columns:

            live_df["event_timestamp"] = pd.to_datetime(
                live_df["event_timestamp"],
                errors="coerce"
            )

            live_df = live_df.sort_values(
                "event_timestamp",
                ascending=False
            )

        # ----------------------------------------------------
        # METRICS
        # ----------------------------------------------------

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Live Records",
            f"{len(live_df):,}"
        )

        if "route" in live_df.columns:

            c2.metric(
                "Live Routes",
                live_df["route"].nunique()
            )

        else:

            c2.metric(
                "Live Routes",
                0
            )

        if "airline" in live_df.columns:

            c3.metric(
                "Airlines",
                live_df["airline"].nunique()
            )

        else:

            c3.metric(
                "Airlines",
                0
            )

        if (
            "event_timestamp" in live_df.columns and
            not live_df["event_timestamp"].dropna().empty
        ):

            latest_time = (
                live_df["event_timestamp"].max()
            )

            c4.metric(
                "Latest Event",
                latest_time.strftime("%H:%M:%S")
            )

        else:

            c4.metric(
                "Latest Event",
                "N/A"
            )

        # ----------------------------------------------------
        # LIVE STATUS
        # ----------------------------------------------------

        st.success(
            "🟢 Live Pipeline Active | "
            "Auto-refreshing every 10 seconds"
        )

        st.caption(
            f"Last UI refresh: "
            f"{pd.Timestamp.now().strftime('%H:%M:%S')}"
        )

        st.divider()

        # ----------------------------------------------------
        # LATEST EVENTS
        # ----------------------------------------------------

        st.subheader(
            "Latest Streaming Flight Events"
        )

        st.dataframe(
            live_df.head(50),
            use_container_width=True,
            height=500
        )

        st.divider()

        # ----------------------------------------------------
        # LIVE FARE MOVEMENT
        # ----------------------------------------------------

        if (
            "event_timestamp" in live_df.columns and
            "price" in live_df.columns
        ):

            st.subheader(
                "Live Fare Movement"
            )

            chart_df = live_df.copy()

            chart_df["price"] = pd.to_numeric(
                chart_df["price"],
                errors="coerce"
            )

            chart_df = chart_df.dropna(
                subset=[
                    "event_timestamp",
                    "price"
                ]
            )

            chart_df = chart_df.sort_values(
                "event_timestamp"
            )

            if not chart_df.empty:

                chart_df = chart_df.set_index(
                    "event_timestamp"
                )

                st.line_chart(
                    chart_df[["price"]]
                )

        # ----------------------------------------------------
        # LATEST ROUTE PRICE SUMMARY
        # ----------------------------------------------------

        if (
            "route" in live_df.columns and
            "price" in live_df.columns
        ):

            st.subheader(
                "Current Average Fare by Route"
            )

            route_summary = (
                live_df
                .groupby("route")["price"]
                .agg(["mean", "min", "max", "count"])
                .reset_index()
                .rename(
                    columns={
                        "mean": "avg_price",
                        "min": "min_price",
                        "max": "max_price",
                        "count": "observations"
                    }
                )
                .sort_values(
                    "avg_price",
                    ascending=False
                )
            )

            route_summary = route_summary.round(2)

            st.dataframe(
                route_summary,
                use_container_width=True
            )

    # --------------------------------------------------------
    # MANUAL REFRESH
    # --------------------------------------------------------

    st.divider()

    if st.button(
        "🔄 Force Refresh Now",
        use_container_width=True
    ):

        st.rerun()
# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "SkyPulse Aviation Analytics | "
    "CDAC DBDA Project | "
    "PySpark + Kafka + Spark SQL + Delta Lake + "
    "Machine Learning + SerpAPI"
)
