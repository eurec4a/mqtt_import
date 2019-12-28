import time
import requests
import datetime
import dateutil.parser as dparser

BUOYS = [
    "SWIFT 16",
    "SWIFT 17",
    "SWIFT 22",
    "SWIFT 23",
    "SWIFT 24",
    "SWIFT 25",
]

def parse_buoy_data(d):
    d = d.copy()
    d["time"] = dparser.parse(d["timestamp"]).replace(tzinfo=None)
    del d["timestamp"]
    for k, v in list(d.items()):
        if v is None:
            del d[k]
    return d

def get_swift_buoy(name, max_age=datetime.timedelta(days=10)):
    now = datetime.datetime.utcnow()
    start = now - max_age
    params = {
        "action": "kml",
        "buoy_name": name,
        "start": start.isoformat(),
        "end": "",
        "format": "json",
    }

    res = requests.get("http://swiftserver.apl.washington.edu/kml", params=params)
    res = res.json()
    if not res.get("success", False):
        raise RuntimeError("unsuccessfull response")
    buoys = [b for b in res["buoys"] if b["name"] == name]
    if len(buoys) != 1:
        raise ValueError("could not uniquely identify buoy \"{}\", {} results".format(name, len(buoys)))
    data = list(sorted(map(parse_buoy_data, buoys[0]["data"]),
                       key=lambda x: x["time"]))

    return data


def _main():
    from mqtt_utils import EUREC4AMqttPublisher

    with EUREC4AMqttPublisher() as publisher:
        while True:
            for buoy in BUOYS:
                try:
                    data = get_swift_buoy(buoy)
                    latest = data[-1]
                except ValueError:
                    continue
                publisher.publish("platform/{}/location".format(buoy),
                    {"time": latest["time"], "lat": latest["lat"], "lon": latest["lon"]},
                    retain=True)
                if "wind_speed" in latest:
                    publisher.publish("platform/{}/wind".format(buoy),
                        {"time": latest["time"], "mag": latest["wind_speed"]},
                        retain=True)
                if "wave_height" in latest:
                    publisher.publish("platform/{}/wave".format(buoy),
                        {"time": latest["time"], "height": latest["wave_height"]},
                        retain=True)
                if "voltage" in latest:
                    publisher.publish("platform/{}/system".format(buoy),
                        {"time": latest["time"], "voltage": latest["voltage"]},
                        retain=True)
                if "img" in latest:
                    publisher.publish("platform/{}/image".format(buoy),
                        {"time": latest["time"], "url": latest["img"]},
                        retain=True)
            time.sleep(30)

if __name__ == "__main__":
    _main()
