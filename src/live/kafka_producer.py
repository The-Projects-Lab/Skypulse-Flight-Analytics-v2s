import os
import pandas as pd
import time
import json

from kafka import KafkaProducer
from fast_flights import FlightQuery, Passengers, create_query, get_flights

from src.live.fast_flights_parser import parse_results


# ==================================================
# PIPELINE STATE
# ==================================================

STATE_FILE = "data/live_pipeline_state.json"


def load_pipeline_state():
    """
    Load producer state.

    If this is the first run for the current calendar day,
    start with Day +1. Repeated runs on the same day advance
    the horizon by one day.
    """

    if not os.path.exists(STATE_FILE):

        return {
            "last_run_date": None,
            "next_days_ahead": 1
        }

    with open(STATE_FILE, "r") as file:
        return json.load(file)


def save_pipeline_state(state):
    """
    Save pipeline state for the next execution.
    """

    os.makedirs(
        os.path.dirname(STATE_FILE),
        exist_ok=True
    )

    with open(STATE_FILE, "w") as file:
        json.dump(
            state,
            file,
            indent=4
        )


# ==================================================
# LOAD CURRENT PIPELINE STATE
# ==================================================

state = load_pipeline_state()

today = pd.Timestamp.today().normalize()

run_date = today.strftime(
    "%Y-%m-%d"
)

if state.get("last_run_date") != run_date:

    days_ahead = 1

else:

    days_ahead = int(state.get(
        "next_days_ahead",
        1
    ))


# ==================================================
# TRAVEL DATE
# ==================================================

travel_date = (
    today
    + pd.Timedelta(days=days_ahead)
).strftime("%Y-%m-%d")


print("\n========================================")
print("SkyPulse Live Flight Producer")
print("========================================")
print("Run Date    :", run_date)
print("Days Ahead  :", days_ahead)
print("Travel Date :", travel_date)
print("========================================\n")


# ==================================================
# KAFKA PRODUCER
# ==================================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==================================================
# LOAD LIVE ROUTES
# ==================================================

routes = pd.read_csv("configs/live_routes.csv")

print("Total routes:", len(routes))


# ==================================================
# CITY TO AIRPORT CODE
# ==================================================

airport_codes = {
    "Delhi": "DEL",
    "Mumbai": "BOM",
    "Bangalore": "BLR",
    "Chennai": "MAA",
    "Hyderabad": "HYD",
    "Kolkata": "CCU"
}


# ==================================================
# CLASSES
# ==================================================

classes = {
    "Economy": "economy",
    "Business": "business"
}


# ==================================================
# COUNTERS
# ==================================================

total_sent = 0
successful_queries = 0
failed_queries = 0


# ==================================================
# PROCESS EVERY ROUTE
# ==================================================

for _, route in routes.iterrows():

    source_city = route["source_city"]
    destination_city = route["destination_city"]

    source = airport_codes[source_city]
    destination = airport_codes[destination_city]

    for class_name, seat in classes.items():

        print(
            f"Fetching: {source_city} → "
            f"{destination_city} | {class_name} "
            f"| Travel Date: {travel_date}"
        )

        try:

            # ------------------------------------------
            # CREATE FAST-FLIGHTS QUERY
            # ------------------------------------------

            query = create_query(
                flights=[
                    FlightQuery(
                        date=travel_date,
                        from_airport=source,
                        to_airport=destination
                    )
                ],
                trip="one-way",
                seat=seat,
                passengers=Passengers(adults=1),
                language="en-US",
                currency="INR"
            )


            # ------------------------------------------
            # GET LIVE FLIGHT RESULTS
            # ------------------------------------------

            results = get_flights(query)


            # ------------------------------------------
            # PARSE AND STANDARDIZE RESULTS
            # ------------------------------------------

            events = parse_results(
                results,
                source_city,
                destination_city,
                travel_date,
                class_name
            )


            print("Flights found:", len(events))


            # Count successful query
            successful_queries += 1


            # ------------------------------------------
            # SEND EVENTS TO KAFKA
            # ------------------------------------------

            for event in events:

                # Ensure days_left matches the
                # automatically selected travel date
                event["days_left"] = days_ahead

                producer.send(
                    "flight_prices_live",
                    value=event
                )

                total_sent += 1


            # Make sure all messages are sent
            producer.flush()


            print(
                "Sent to Kafka:",
                len(events)
            )


        except Exception as e:

            failed_queries += 1

            print(
                f"Skipping: {source_city} → "
                f"{destination_city} | {class_name}"
            )

            print("Error:", e)


        # ----------------------------------------------
        # SMALL DELAY BETWEEN API REQUESTS
        # ----------------------------------------------

        time.sleep(2)


# ==================================================
# CLOSE PRODUCER
# ==================================================

producer.close()


# ==================================================
# FINAL PIPELINE METRICS
# ==================================================

print("\n--------------------------------")
print("Live streaming completed.")
print("--------------------------------")

print("Days ahead:", days_ahead)
print("Travel date:", travel_date)
print("Total routes:", len(routes))
print("Total route/class queries:", len(routes) * len(classes))
print("Successful queries:", successful_queries)
print("Failed queries:", failed_queries)
print("Total events sent to Kafka:", total_sent)
print("--------------------------------")


# ==================================================
# ADVANCE TO NEXT DAY
# Always advance, even if some queries failed
# ==================================================

state["last_run_date"] = run_date

state["next_days_ahead"] = days_ahead + 1

save_pipeline_state(state)

print("\n========================================")

if failed_queries == 0:
    print("Pipeline completed successfully.")
else:
    print("Pipeline completed with some failed queries.")
    print(
        f"Failed queries: {failed_queries}"
    )
    print(
        "Failed queries will not be automatically retried."
    )

print(
    f"Next run will automatically use "
    f"Day +{days_ahead + 1}"
)

print("========================================")
