import datetime
from httptail import http_iter
import pynmea2

class NMEAAssetState(object):
    def __init__(self):
        self.time_of_day = None
        self.lat = None
        self.lon = None
        self.gound_speed = None
        self.heading = None
        self.day = None
        self.timezone = None
    def update(self, msg):
        self.time_of_day = getattr(msg, "timestamp", self.time_of_day)
        self.lat = getattr(msg, "latitude", self.lat)
        self.lon = getattr(msg, "longitude", self.lon)
        try:
            self.ground_speed = float(msg.spd_over_grnd_kts) * 1.852 / 3.6 # knots to m/s (exact)
        except AttributeError:
            pass
        self.heading = getattr(msg, "true_track", self.heading)
        self.day = getattr(msg, "datestamp", self.day)
        try:
            tz = msg.tzinfo
            if tz.hh is not None and tz.mm is not None:
                self.timezone = tz
            else:
                self.timezone = None
        except AttributeError:
            pass

    @property
    def timestamp(self):
        dt = datetime.datetime.combine(self.day, self.time_of_day)
        if self.timezone is not None:
            dt = dt.replace(tzinfo=self.timezone)
        return dt

    def to_dict(self):
        res = {"time": self.timestamp}
        for var in ["lat", "lon", "ground_speed", "heading"]:
            val = getattr(self, var, None)
            if val is not None:
                res[var] = val
        return res

    def __str__(self):
        return "{} {} {} {} {}".format(self.timestamp, self.lat, self.lon, self.sog, self.heading)

def _main():
    from mqtt_utils import EUREC4AMqttPublisher

    streamreader = pynmea2.NMEAStreamReader()
    assets = ["33RO"]
    sources = {asset: http_iter("https://seb.noaa.gov/pub/flight/aamps_ingest/ship/{}.txt".format(asset))
               for asset in assets}
    asset_states = {asset: NMEAAssetState() for asset in assets}
    asset_platform_ids = {
        "33RO": "RHB"
    }

    with EUREC4AMqttPublisher() as publisher:
        while True:
            for asset in assets:
                for msg in streamreader.next(next(sources[asset]).decode("ascii")):
                    asset_states[asset].update(msg)

                publisher.publish("platform/{}/location".format(asset_platform_ids[asset]),
                    asset_states[asset].to_dict(),
                    retain=True)

if __name__ == "__main__":
    _main()
