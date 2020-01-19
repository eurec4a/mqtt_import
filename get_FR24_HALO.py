import datetime
import time
import json
import requests

from mqtt_import.mqtt_utils import EUREC4AMqttPublisher

class NoSuchAircraftError(ValueError):
    pass

class FR24_scraper(object):
    def __init__(self):

        self.url = "https://data-live.flightradar24.com/zones/fcgi/feed.js?bounds=15.73,12.90,-64.55,-49.82&faa=1&satellite=1&mlat=1&flarm=1&adsb=1&gnd=1&air=1&vehicles=1&estimated=1&maxage=14400&gliders=1&stats=1&selected=23933062&ems=1"

        self.update()

    def update(self):
        # FR24 needs certain headers, but it seems like, they don't nee the Cookie
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Cache-Control': 'max-age=0',
            #'Cookie': '__cfduid=d5b1ed2cfbce749611b239466ffafff961579436236; FR24ID=ohvk8noejjq824gmtk7cbak34r9p06jghii4stjsd6g8s62ef5ik; _frpk=_tjhbkQEQ8KQg2czy8el-g; cookie_law_consent=1',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': 'data-live.flightradar24.com',
            'TE': 'Trailers',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0'
        }

        res = requests.get(self.url, headers=headers)
        res.raise_for_status()
        self.data = json.loads(res.text)

    def get_location(self, callsign):
        self.update()
        for key, value in self.data.items():
            if key in ('full_count', 'version', 'stats', 'selected-aircraft'):
                continue
            if value[9] != callsign:
                continue

            location = {
                "time": datetime.datetime.fromtimestamp(value[10], datetime.timezone.utc),
                "lat": float(value[1]),
                "lon": float(value[2]),
                "heading": float(value[3]),
            #    "ground_speed": "<platform speed over ground, unit: m/s>",
                "press_alt": float(value[4]),
            #    "gps_msl_alt": "<unit: m>",
            #    "wgs84_alt": "<altitude in WGS84 coordinates, unit: m>",
            #    "vert_velocity": "<vertical velocity, unit: m/s>",
            #    "type": "fixed"
            }
            return location

        raise NoSuchAircraftError




def main():
    ############

    UPDATE_INTERVAL = 30

    ############
    with EUREC4AMqttPublisher() as publisher:
        fr24 = FR24_scraper()
        while True:
            last_update = datetime.datetime.utcnow()

            try:
                location = fr24.get_location('D-ADLR')
            except NoSuchAircraftError as e:
                print('NoSuchAircraftError:', e)
            else:
                topic = 'platform/HALO/'
                print('publish', topic)
                publisher.publish(topic+'location', location, True)

            print('Last update: {}, next update in {} seconds.'.format(
                last_update.isoformat(), UPDATE_INTERVAL
            ))

            time.sleep(
                UPDATE_INTERVAL - (datetime.datetime.utcnow() - last_update).total_seconds()
            )

if __name__ == '__main__':
    main()