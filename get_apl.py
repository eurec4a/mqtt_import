import time
import datetime
import requests
from io import StringIO

# must be exported VehicleParsedOutput Records filtered to the corresponding vehicle
EXPORT_IDS = [
    {
        "platform_id": "SV3-245",
        #"wgms_record_id": 74383,
        "wgms_record_id": 74446,
    },
    {
        "platform_id": "SV3-247",
        #"wgms_record_id": 74297,
        "wgms_record_id": 74445,
    },
]

LOGINSOAP="""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap=
"http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CreateLoginSession xmlns="http://gliders.liquidr.com/webservicesWebServices">
      <login>{login}</login>
      <password>{password}</password>
      <orgName>{org}</orgName>
    </CreateLoginSession>
  </soap:Body>
</soap:Envelope>
"""

# AVAILABLE FIELS:
#
# TimeStamp
# Structure ID
# Ground Speed(kt)
# Target
# Desired Heading (deg)
# Sub Heading
# POG (deg)
# Float Temp(degC)
# Pressure Sensor Float(kPa)
# Battery (Wh)
# Signal Strength
# User Name
# Lat (deg)
# Lon (deg)
# Distance Over Ground(m)
# Water Speed (kt/1000) WS
# Water Direction
# Current Speed (kt)
# Current Heading (deg)
# Created On

def parse_time(s):
    if len(s) == 0:
        return float("nan")
    return datetime.datetime.strptime(s, "%m/%d/%Y %H:%M:%S")

def parse_knots(s):
    if len(s) == 0:
        return float("nan")
    return float(s) * 1.852 / 3.6 # knots to m/s (exact)

def parse_milliknots(s):
    if len(s) == 0:
        return float("nan")
    return float(s) * 1.852 / 3.6 / 1000. # milliknots to m/s (exact)

def parse_degrees(s):
    if len(s) == 0:
        return float("nan")
    return float(s)

def parse_celsius(s):
    if len(s) == 0:
        return float("nan")
    return float(s)

def parse_kilopascal(s):
    if len(s) == 0:
        return float("nan")
    return float(s) * 1000. # kPa to Pa

def parse_watt_hours(s):
    if len(s) == 0:
        return float("nan")
    return float(s)

def parse_lat(s):
    if len(s) == 0:
        return float("nan")
    return float(s)

def parse_lon(s):
    if len(s) == 0:
        return float("nan")
    return float(s)

FIELDMAP = {
        "TimeStamp": {
            "name": "time",
            "parser": parse_time,
        },
        "Ground Speed(kt)": {
            "name": "speed_over_ground",
            "parser": parse_knots,
        },
        "Desired Heading (deg)": {
            "name": "desired_heading",
            "parser": parse_degrees,
        },
        "Float Temp(degC)": {
            "name": "device_temperature",
            "parser": parse_celsius,
        },
        "Pressure Sensor Float(kPa)": {
            "name": "pressure",
            "parser": parse_kilopascal,
        },
        "Battery (Wh)": {
            "name": "battery_charge",
            "parser": parse_watt_hours,
        },
        "Lat (deg)": {
            "name": "lat",
            "parser": parse_lat,
        },
        "Lon (deg)": {
            "name": "lon",
            "parser": parse_lon,
        },
        "Water Speed (kt/1000) WS": {
            "name": "water_speed",
            "parser": parse_milliknots,
        },
        "Water Direction": {
            "name": "water_direction",
            "parser": parse_degrees,
        },
        "Current Speed (kt)": {
            "name": "speed_in_water",
            "parser": parse_knots,
        },
        "Current Heading (deg)": {
            "name": "heading",
            "parser": parse_degrees,
        },
        "Created On": {
            "name": "dataset_time",
            "parser": parse_time,
        },
}

class GliderApi(object):
    def __init__(self, authinfo, base_url="https://apl-uw.wgms.com"):
        self._base_url = base_url
        #curl -k -H "Content-Type: text/xml; charset=utf-8" --dump-header headers -H "SOAPAction:" -d @loginsoap.xml -X POST https://gliders.wgms.com/webservices/entityapi.asmx
        loginsoap = LOGINSOAP.format(**authinfo)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": None,
        }
        self._session = requests.Session()
        res = self._session.post(
            self._base_url + "/webservices/entityapi.asmx",
            data=loginsoap,
            headers=headers)
        res.raise_for_status()

    def get_export(self, view, entity):
        #http://apl-uw.wgms.com/pages/exportPage.aspx?viewid=74297&entitytype=42
        res = self._session.get(
                self._base_url + "/pages/exportPage.aspx",
                params={
                    "viewid": view,
                    "entitytype": entity
                })
        lines = iter(StringIO(res.text))
        header = next(lines).strip().split(",")
        for line in lines:
            parts = line.strip().split(",")
            data = {}
            for col, part in zip(header, parts):
                try:
                    colinfo = FIELDMAP[col]
                except KeyError:
                    continue
                data[colinfo["name"]] = colinfo["parser"](part.strip())
            yield data

def _main():
    from mqtt_utils import get_config_dir, EUREC4AMqttPublisher
    import os
    import json
    with open(os.path.join(get_config_dir(), "liquidr_gliders.json")) as configfile:
        config = json.load(configfile)

    api = GliderApi(config)


    with EUREC4AMqttPublisher() as publisher:
        while True:
            for vehicle in EXPORT_IDS:
                entries = list(sorted(api.get_export(vehicle["wgms_record_id"], 42),
                                      key=lambda x: x["time"]))
                latest = entries[-1]
                publisher.publish("platform/{}/location".format(vehicle["platform_id"]),
                    {"time": latest["time"], "lat": latest["lat"], "lon": latest["lon"]},
                    retain=True)
            time.sleep(30)

if __name__ == "__main__":
    _main()
