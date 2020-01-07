import datetime
from mqtt_utils import get_mqtt_client

def _main():
    import sys
    client = get_mqtt_client()

    logfile = open(sys.argv[1], "a")

    def on_connect(client, userdata, flags, rc):
        print("MQTT: connected with result code {}".format(rc))
        client.subscribe("#")
    def on_disconnect(client, userdata, rc):
        print("MQTT: disonnected with result code {}".format(rc))
    def on_message(client, userdata, msg):
        now = datetime.datetime.utcnow()
        logfile.write("{} {} {}\n".format(now.isoformat(), msg.topic, msg.payload))
    def on_log(client, userdata, level, buf):
        print("#LOG: {} {}".format(level, buf))

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    #client.on_log = on_log
    print("initial connect: {}".format(client.connect("mqtt.eurec4a.eu", 8883, 60)))
    client.loop_forever()

if __name__ == "__main__":
    _main()
