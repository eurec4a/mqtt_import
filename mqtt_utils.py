import numpy as np
import time
import datetime
import json

def json_default(obj):
    if isinstance(obj, np.datetime64):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    else:
        return obj

def get_config_dir():
    import os
    if "EUREC4A_CONFIG" in os.environ:
        return os.path.join(os.environ["EUREC4A_CONFIG"])
    elif "XDG_CONFIG_PATH" in os.environ:
        return os.path.join(os.environ["XDG_CONFIG_HOME"], "eurec4a")
    elif "HOME" in os.environ:
        return os.path.join(os.environ["HOME"], ".config", "eurec4a")
    else:
        raise RuntimeError("could not find config path")


class MQTTDeduplicator(object):
    def __init__(self, expiration_time = np.timedelta64(15, "m")):
        self.expiration_time = expiration_time
        self.messages = {}

    def is_new(self, topic, message):
        now = np.datetime64("now")
        if not topic in self.messages:
            self.messages[topic] = (now, message)
            return True
        last_update, last_message = self.messages[topic]
        if last_update + self.expiration_time < now:
            self.messages[topic] = (now, message)
            return True
        if last_message != message:
            self.messages[topic] = (now, message)
            return True
        return False

class EUREC4AMqttPublisher(object):
    def __init__(self, username=None, password=None, deduplicate=True):
        import paho.mqtt.client as mqtt
        import os
        self.client = mqtt.Client()
        self.client.tls_set(ca_certs=os.path.join(os.path.dirname(__file__), "trustid-x3-root.pem.txt"))
        if username is None and password is None:
            config_dir = get_config_dir()
            with open(os.path.join(config_dir, "mqtt_import.json")) as configfile:
                config = json.load(configfile)
            username = config["username"]
            password = config["password"]
        self.client.username_pw_set(username, password)
        self._is_connected = False
        if deduplicate:
            self.deduplicator = MQTTDeduplicator()
        else:
            self.deduplicator = None

    def __enter__(self):
        self.client.connect("mqtt.eurec4a.eu", 8883, 60)
        self.client.loop_start()
        return self

    def __exit__(self, type, value, tb):
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, topic, data, retain=False):
        if self.deduplicator is not None:
            if not self.deduplicator.is_new(topic, data):
                return
            self.client.publish(topic, json.dumps(data, default=json_default), retain=retain)
            print(topic, data)
