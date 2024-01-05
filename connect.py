import sqlite3
import paho.mqtt.client as mqtt
import os
import time

broker_address = "XX.XX.XX.XX"
mqttuser="XXXXXX"
mqttpassword="XXXXXX"
# ścieżka do db
dir_path = r"C:\Users\Administrator\Desktop"
db_path = os.path.join(dir_path, "sqlite.db")

# -----------------------------------------------

def get_topic():
    topic_list = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT `Key` FROM Devices")
        results = cursor.fetchall()
        topic_list = [row[0] for row in results]
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return topic_list

def on_connect(client, userdata, flags, rc):
    if not rc:
        print("Połączono z MQTT \n")
        for topic in get_topic():
            client.subscribe(topic)
    else: print("Blad z MQTT")

def send_message(client, topic, message):
    payload = message
    client.publish(topic, payload)
    print(f"Wysłano wiadomość na temat - {topic}: {payload}\n")

# wiadomość: 1,"idCommand",data(d.m.r g:m)
def harmonogram(client):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # wyciągam wszystkie daty wszystkich topiców
        cursor.execute("SELECT * FROM Harmonogram")
        result = cursor.fetchall()
        for row in result:
            idd, topic, command, date = row
            print(f"Temat: {topic}, data: {date}, command: {command}\n")
            current_time = time.strftime("%d.%m.%Y %H:%M", time.localtime())
            print(current_time,date)
            if str(date) <= str(current_time):
                com = "2,"+str(command)+","
                send_message(client, topic, com)
                print(f"Wysłano na topic {topic} komendę {command}")
                cursor.execute("INSERT INTO History (topic, idCommand, date, done) VALUES (?, ?, ?, ?)", (topic, command, date, 1))
                conn.commit()
                cursor.execute("DELETE FROM Harmonogram WHERE id=?", (idd,))
                conn.commit()
            else:
               # print(f"Nie odpowiednia godzina: czas z bazy: {date} czas systemowy: {current_time}")
               pass
    except sqlite3.Error as e:
        print(f"Błąd bazy danych {e}\n")
    finally:
        if conn:
            cursor.close()
            conn.close()

# wiadomość: 2,"idCommand"
def history(client, topic, message):
    try:
        conn=sqlite3.connect(db_path)
        cursor=conn.cursor()
        current_time = time.strftime("%d.%m.%Y %H:%M", time.localtime())
        print(f"Wysłano na topic {topic} komendę {message[1]}")
        cursor.execute("INSERT INTO History (topic, idCommand, date, done) VALUES (?, ?, ?, ?)",
        (topic, message[1], current_time, 1))
        conn.commit()

    except sqlite3.Error as e:
        print(f"Błąd bazy danych {e}\n")
    finally:
        if conn:
            cursor.close()
            conn.close()

def on_message(client, userdata, msg):
    print(f"Otrzymano wiadomość na temacie {msg.topic}: {msg.payload.decode()}\n")
    message = msg.payload.decode().split(",")
    # 1 -> do harmonogramu
    # topic, komenda, data wykonania
    if message[0] == '1':
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # message[1] - komenda message[2]- data wykonania
            cursor.execute("INSERT INTO Harmonogram (topic, idCommand, date) VALUES (?, ?, ?)",
                           (msg.topic, message[1], message[2]))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Błąd bazy danych {e}\n")
        finally:
            if conn:
                cursor.close()
                conn.close()
    # 2 -> z telefonu(history, arduino)
    elif message[0] == '2':
        history(client, msg.topic, message)
    # 3 -> 
    elif int(message[0]) >=300000 and int(message[0]) <=311111:
        print(f"Pin info: {message[0]}\n")
    else:
        print(f"Nieznana zmienna sterująca: {message[0]}\n")
        print("Obsługiwane to:\n1 do harmonogramu\n2 z harmonogramu\n")

# -----------------------------------------------

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
    while True:
        harmonogram(client)
        time.sleep(2)

except sqlite3.Error as e:
    print(f"Błąd bazy: {e}")


finally:
    if client and client.is_connected():
        client.loop_stop()
        client.disconnect()
    if conn:
        conn.close()
