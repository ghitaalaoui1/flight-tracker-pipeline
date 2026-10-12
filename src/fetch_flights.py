import requests

from src.storage.mongo import save_planes

URL = "https://opensky-network.org/api/states/all"
BBOX_MAROC = {"lamin": 27.6, "lomin": -13.2, "lamax": 35.9, "lomax": -1.0}


def fetch_states(bbox):
    try:
        response = requests.get(URL, params=bbox, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Erreur réseau :", e)
        return []
    return response.json()["states"] or []


def parse_state(state):
    speed_ms = state[9]
    return {
        "icao24": state[0],
        "callsign": (state[1] or "").strip(),
        "country": state[2],
        "timestamp": state[3],
        "longitude": state[5],
        "latitude": state[6],
        "altitude_m": state[7],
        "on_ground": state[8],
        "speed_kmh": round(speed_ms * 3.6) if speed_ms is not None else None,
        "vertical_rate_ms": state[11],
        "squawk": state[14],
    }


def is_valid(plane):
    return plane["latitude"] is not None and plane["longitude"] is not None


def main():
    states = fetch_states(BBOX_MAROC)
    planes = [parse_state(s) for s in states]
    planes = [p for p in planes if is_valid(p)]
    print("Avions valides :", len(planes))
    for p in planes:
        name = p["callsign"] or p["icao24"]
        print(name, "|", p["country"], "|", p["altitude_m"], "m |", p["speed_kmh"], "km/h")
    inserted = save_planes(planes)
    print("Nouvelles positions enregistrées :", inserted)


if __name__ == "__main__":
    main()