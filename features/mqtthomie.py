"""
    Send SMA values to mqtt broker.

    2018-12-23 Tommi2Day
    2019-03-02 david-m-m
    2020-09-22 Tommi2Day ssl support
    2021-01-07 sellth added support for multiple inverters
    2024-12-31 Fork for MQTT homie structure
    
    Configuration:

    [FEATURE-mqtt]
    # MQTT broker details
    mqtthost=mqtt
    mqttport=1883
    #mqttuser=
    #mqttpass=
    mqttfields=pconsume,psupply,p1consume,p2consume,p3consume,p1supply,p2supply,p3supply
    #topic will be exted3ed with serial
    mqtttopic=SMA-EM/status
    pvtopic=SMA-PV/status
    # publish all values as single topics (0 or 1)
    publish_single=1
    # How frequently to send updates over (defaults to 20 sec)
    min_update=30
    #debug output
    debug=0

    # ssl support
    # adopt mqttport above to your ssl enabled mqtt port, usually 8883
    # options:
    # activate without certs=use tls_insecure
    # activate with ca_file, but without client_certs
    ssl_activate=0
    # ca file to verify
    ssl_ca_file=ca.crt
    # client certs
    ssl_certfile=
    ssl_keyfile=
    #TLSv1.1 or TLSv1.2 (default 2)
    tls_protocol=2

"""

import paho.mqtt.client as mqtt
import paho.mqtt as pahodef
import platform
import json
import time
import ssl
import traceback

mqtt_last_update = 0
mqtt_debug = 0
mqtt_homie_topic = "homie/"

def run(emparts, config):
    global mqtt_last_update
    global mqtt_debug

    # Only update every X seconds
    if time.time() < mqtt_last_update + int(config.get('min_update', 20)):
        if (mqtt_debug > 1):
            print("mqtt homie: data skipping")
        return

    # prepare mqtt settings
    mqtthost = config.get('mqtthost', 'mqtt')
    mqttport = config.get('mqttport', 1883)
    mqttuser = config.get('mqttuser', None)
    mqttpass = config.get('mqttpass', None)
    mqtttopic = config.get('mqtttopic', "SMA-EM")
    mqttfields = config.get('mqttfields', 'pconsume,psupply')
    mqtt_inverter_topic = config.get('invertertopic', "SMA-PV")

    ssl_activate = config.get('ssl_activate', False)
    ssl_ca_file = config.get('ssl_ca_file', None)
    ssl_certfile = config.get('ssl_certfile', None)
    ssl_keyfile = config.get('ssl_keyfile', None)
    tls_protocol = config.get('tls_protocol', "2")
    if tls_protocol == "1":
        tls = ssl.PROTOCOL_TLSv1_1
    elif tls_protocol == "2":
        tls = ssl.PROTOCOL_TLSv1_2
    else:
        tls = ssl.PROTOCOL_TLSv1_2
        if mqtt_debug > 0:
            print("tls_protocol %s unsupported, use (TLSv1.)2" % tls_protocol)

    # mqtt client settings
    myhostname = platform.node()
    mqtt_clientID = 'SMA-EM@' + myhostname
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, mqtt_clientID)
    if None not in [mqttuser, mqttpass]:
        client.username_pw_set(username=mqttuser, password=mqttpass)

    if ssl_activate == "1":
        # and ssl_ca_file:
        if ssl_certfile and ssl_keyfile and ssl_ca_file:
            # use client cert
            client.tls_set(ssl_ca_file, certfile=ssl_certfile, keyfile=ssl_keyfile, tls_version=tls)
            if mqtt_debug > 0:
                print("mqtt homie: ssl ca and client verify enabled")
        elif ssl_ca_file:
            # no client cert
            client.tls_set(ssl_ca_file, tls_version=tls)
            if mqtt_debug > 0:
                print("mqtt homie: ssl ca verify enabled")
        else:
            # disable certificat verify as there is no certificate
            client.tls_set(tls_version=tls)
            client.tls_insecure_set(True)
            if mqtt_debug > 0:
                print("mqtt homie: ssl verify disabled")
    else:
        if mqtt_debug > 0:
            print("mqtt homie: ssl disabled")

    # last aupdate
    # last aupdate
    mqtt_last_update = time.time()

    # energy meter
    serial = emparts['serial']
    data = {}
    for f in mqttfields.split(','):
        data[f] = emparts.get(f, 0)

    data_with_units = extract_values_and_units(emparts)

    # add pv data
    pvpower = 0
    daily = 0
    try:
        from features.pvdata import pv_data

        for inv in pv_data:
            # handle missing data during night hours
            if inv.get("AC Power") is None:
                pass
            elif inv.get("DeviceClass") in ("Solar Inverter", "Hybrid Inverter"):
                pvpower += inv.get("ACPower", 0)
                # NOTE: daily yield is broken for some inverters
                daily += inv.get("dailyYield", 0)

        pconsume = emparts.get('pconsume', 0)
        psupply = emparts.get('psupply', 0)
        pusage = pvpower + pconsume - psupply
        data['pvsum'] = pvpower
        data['pusage'] = pusage
        data['pvdaily'] = daily
    except:
        pv_data = None
        pass

    data['timestamp'] = mqtt_last_update
    payload = json.dumps(data)
    topic = mqtt_homie_topic + mqtttopic + str(serial)
    try:
        # mqtt connect
        client.connect(str(mqtthost), int(mqttport))
        client.loop_start()

        # Define MQTT Topics
        topic_homie = f"{topic}/$homie"
        value_homie = f"4.0"
        topic_name = f"{topic}/$name"
        topic_state = f"{topic}/$state"
        value_state = f"ready"
        topic_nodes = f"{topic}/$nodes"
        value_nodes = f"meter" 
        topic_node = f"{topic}/meter"
        topic_node_name = f"{topic_node}/$name"
        value_node_name = f"SMA HomeManager2.0"
        topic_node_properties = f"{topic_node}/$properties"
        value_node_properties = mqttfields
        client.publish(topic_homie, value_homie)
        client.publish(topic_name, "SMA energy meter")
        client.publish(topic_state, value_state)
        client.publish(topic_nodes, value_nodes)
        client.publish(topic_node_name, value_node_name)
        client.publish(topic_node_properties, value_node_properties)

        #client.publish(topic, payload)
        if mqtt_debug > 0:
            print("mqtt homie: sma-em topic %s data published %s:%s" % (topic,
                                                                  format(time.strftime("%H:%M:%S", time.localtime(
                                                                      mqtt_last_update))), payload))

        for item in data_with_units.keys():
            itemtopic = topic_node + '/' + item
            item_topic_name = f"{itemtopic}/$name"
            item_topic_unit = f"{itemtopic}/$unit"
            item_topic_datatype = f"{itemtopic}/$datatype"

            if mqtt_debug > 0:
                print("mqtt homie: publishing %s:%s" % (itemtopic, data_with_units[item]["value"]))

            client.publish(itemtopic, str(data_with_units[item]["value"]))
            client.publish(item_topic_name, item)
            client.publish(item_topic_unit, data_with_units[item]["unit"])
            client.publish(item_topic_datatype, "float")

        # pvoption
        # inverter
        # mqttpvtopic = mqtt_homie_topic + config.get('pvtopic', "SMD-PV")
        pvserial = inv.get("serial").get("value")
        inverter_topic = mqtt_homie_topic + mqtt_inverter_topic + str(pvserial)
        if None not in [pv_data, inverter_topic]:
            inverter_topic_homie = f"{inverter_topic}/$homie"
            inverter_value_homie = f"4.0"
            inverter_topic_name = f"{inverter_topic}/$name"
            inverter_topic_state = f"{inverter_topic}/$state"
            inverter_value_state = f"ready"
            inverter_topic_nodes = f"{inverter_topic}/$nodes"
            inverter_value_nodes = f"meter"
            inverter_topic_node = f"{inverter_topic}/meter"
            inverter_topic_node_name = f"{inverter_topic_node}/$name"
            inverter_value_node_name = f"SMA Inverter"
            inverter_topic_node_properties = f"{inverter_topic_node}/$properties"
            
            client.publish(inverter_topic_homie, inverter_value_homie)
            client.publish(inverter_topic_name, "SMA Inverter")
            client.publish(inverter_topic_state, inverter_value_state)
            client.publish(inverter_topic_nodes, inverter_value_nodes)
            client.publish(inverter_topic_node_name, inverter_value_node_name)
                 
            if pv_data is not None:
                for inv_data in pv_data:
                    inv_mqtt_properties = ""
                    for item in inv_data.keys():

                        if item == "timestamp":
                            continue

                        if inv_mqtt_properties != "":
                            inv_mqtt_properties += ","
                        inv_mqtt_properties += item

                    client.publish(inverter_topic_node_properties, inv_mqtt_properties)   

                for inv_data in pv_data:
                    inv_mqtt_properties = ""
                    for item in inv_data.keys():

                        if item == "timestamp":
                            continue

                        itemtopic = inverter_topic_node + '/' + item
                        item_topic_name = f"{itemtopic}/$name"
                        item_topic_unit = f"{itemtopic}/$unit"
                        item_topic_datatype = f"{itemtopic}/$datatype"

                        if mqtt_debug > 0:
                            print("mqtt homie: publishing %s:%s %s" % (itemtopic, inv_data.get(item).get("value"), inv_data.get(item).get("unit")))
                       
                        client.publish(item_topic_name, item)
                        client.publish(itemtopic, str(inv_data.get(item).get("value")))
                        client.publish(item_topic_unit, str(inv_data.get(item).get("unit")))
                        client.publish(item_topic_datatype, str(inv_data.get(item).get("type")))

                    # pvserial = inv.get("serial").get("value")
                    # pvtopic = mqttpvtopic + str(pvserial)
                    # payload = json.dumps(inv)
                    # # sendf pv topic
                    # client.publish(pvtopic, payload)
                    # if mqtt_debug > 0:
                    #     print("mqtt homie: sma-pv topic %s data published %s:%s" % (
                    #         pvtopic,
                    #         format(time.strftime("%H:%M:%S",
                    #                              time.localtime(
                    #                                  mqtt_last_update))),
                    #         payload))
    
                   

        client.loop_stop()
        client.disconnect()

    except Exception as e:
        print("mqtt homie: Error publishing")
        print(traceback.format_exc())
        pass


# Function to extract values and units
def extract_values_and_units(data):
    result = {}
    
    for key, value in data.items():
        if 'unit' in key:  # If the key contains 'unit', it's the unit
            value_key = key.replace('unit', '')  # Remove 'unit' to get the corresponding value key
            if value_key in data:  # Check if the corresponding value exists
                result[value_key] = {'value': data[value_key], 'unit': value}
    
    return result

# Function to extract inverter values and units
def extract_inverter_values_and_units(data):
    result = {}
    
    for key, value in data.items():
        if 'unit' in key:  # If the key contains 'unit', it's the unit
            value_key = key.replace('unit', '')  # Remove 'unit' to get the corresponding value key
            if value_key in data:  # Check if the corresponding value exists
                result[value_key] = {'value': data[value_key], 'unit': value}
    
    return result


def stopping(emparts, config):
    pass


def config(config):
    global mqtt_debug
    mqtt_debug = int(config.get('debug', 0))
    print('mqtt homie: feature enabled')
