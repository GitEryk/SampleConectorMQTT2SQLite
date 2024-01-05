import sqlite3
import paho.mqtt.client as mqtt
import os
import time


# MAIN
try:
    print("Utworzono obiekt MQTT\n")
    client = mqtt.Client()
    client.username_pw_set(mqttuser, mqttpassword)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_address, 1883, 60)
    client.loop_start()

    # uruchamianie harmonogramu
    while 1==1:
        #harmonogram(client)
        time.sleep(2)

except sqlite3.Error as e:
    print(f"Błąd bazy: {e}")

except KeyboardInterrupt:
    if client and client.is_connected():
        client.loop_stop()
        client.disconnect()
    print("Zamknięcie programu Ctrl+C\n")
finally:
    if client and client.is_connected():
        client.loop_stop()
        client.disconnect()
    if conn:
        conn.close()
