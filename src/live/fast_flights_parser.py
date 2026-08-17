from datetime import datetime
import uuid


def parse_results(results, source_city, destination_city, travel_date, seat_class):

    events = []

    for flight in results:

        if not flight.flights:
            continue

        first_leg = flight.flights[0]
        last_leg = flight.flights[-1]

        # Number of stops
        stop_count = len(flight.flights) - 1

        if stop_count == 0:
            stops = "zero"
        elif stop_count == 1:
            stops = "one"
        else:
            stops = "two_or_more"

        # Departure time
        departure = first_leg.departure.time

        departure_hour = departure[0] if departure else 0
        departure_minute = departure[1] if len(departure) > 1 else 0

        if departure_hour is None:
            departure_hour = 0

        departure_time = f"{departure_hour:02d}:{departure_minute:02d}"

        # Arrival time
        arrival = last_leg.arrival.time

        arrival_hour = arrival[0] if arrival else 0
        arrival_minute = arrival[1] if len(arrival) > 1 else 0

        if arrival_hour is None:
            arrival_hour = 0

        arrival_time = f"{arrival_hour:02d}:{arrival_minute:02d}"

        # Total duration
        duration = sum(
            leg.duration
            for leg in flight.flights
        )

        # Days left
        travel = datetime.strptime(
            travel_date,
            "%Y-%m-%d"
        ).date()

        days_left = (
            travel - datetime.now().date()
        ).days

        event = {
            "event_id": str(uuid.uuid4()),
            "observed_at": datetime.now().isoformat(),

            "source_city": source_city,
            "destination_city": destination_city,
            "travel_date": travel_date,

            "airline": flight.airlines[0] if flight.airlines else None,
            "airline_code": flight.type,

            "departure_time": departure_time,
            "arrival_time": arrival_time,

            "stops": stops,
            "duration": duration,

            "class": seat_class,

            "price": flight.price,
            "currency": "INR",

            "days_left": days_left
        }

        events.append(event)

    return events
