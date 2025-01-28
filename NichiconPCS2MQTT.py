import asyncio
import sys
from pychonet.lib.udpserver import UDPServer
from pychonet import ECHONETAPIClient as api
from pychonet.lib.const import (ENL_GETMAP, ENL_MANUFACTURER, ENL_SETMAP,
                                ENL_UID)
from pychonet.lib.eojx import EOJX_CLASS, EOJX_GROUP
from pychonet.lib.epc import EPC_CODE, EPC_SUPER
import ctypes
from pychonet import Factory
from datetime import datetime
import paho.mqtt.client as mqtt
import time
import configparser
import os
import warnings

# Suppress all DeprecationWarnings
warnings.filterwarnings("ignore", category=DeprecationWarning)




def epc2str(gc, cc, pc):
    try:
        return EPC_SUPER[pc]
    except KeyError:
        pass

    try:
        return EPC_CODE[gc][cc][pc]
    except KeyError:
        return "Unknown"


async def main(argv):
    # Load configuration from a file
    config = configparser.ConfigParser()
    config_file = 'config.ini'

    if not os.path.exists(config_file):
        print(f"Config file '{config_file}' not found.")
        exit(1)
    config.read(config_file)

    # Extract configuration values
    try:
        broker_address = config['MQTT']['BrokerAddress']
        broker_port = int(config['MQTT']['BrokerPort'])
        broker_user = config['MQTT']['User']
        broker_password = config['MQTT']['Password']
        sensor_data_path = config['MQTT']['DataPath']  # This is the MQTT topic path

        pollingInterval = int(config['Echonet']['PollingIntervalInSeconds'])
        pcs_IP=config['Echonet']['PCS_IP']

    except KeyError as e:
        print(f"Missing configuration key: {e}")
        exit(1)

    # Initialize MQTT client
    mqttClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,"Nichicon_MultiInputPCS")
    # Set username and password for broker authentication
    mqttClient.username_pw_set(broker_user, broker_password)

    # Callback functions
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to broker")
        else:
            print(f"Failed to connect, return code {rc}")

    def on_disconnect(client, userdata, rc):
        if rc != 0:
            print("Unexpected disconnection, trying to reconnect...")

    # Attach callbacks to client
    mqttClient.on_connect = on_connect
    mqttClient.on_disconnect = on_disconnect

    mqttClient.reconnect_delay_set(min_delay=1, max_delay=120)  # Set retry delay between 1s to 2 mins
    mqttClient.loop_start()  # Start background thread to manage connections

    connected = False
    while not connected:
        try:
            mqttClient.connect(broker_address, broker_port)
            connected = True
        except:
            print("Broker not available, retrying in 5 seconds...")
            time.sleep(5)

    # Main loop to publish sensor data



    udp = UDPServer()
    loop = asyncio.get_event_loop()
    udp.run("0.0.0.0", 3610, loop=loop)
    server = api(udp)
    server._debug_flag = False
    server._message_timeout = 30
    target = pcs_IP

    await server.discover(target)
    # Timeout after 3 seconds
    for x in range(0, 300):
        await asyncio.sleep(0.01)
        if "discovered" in list(server._state[target]):
            break

    instance_list = []
    state = server._state[target]

    for eojgc in state["instances"].keys():
        for eojcc in state["instances"][eojgc].keys():
            for instance in state["instances"][eojgc][eojcc].keys():
                # issue here??
                await server.getAllPropertyMaps(target, eojgc, eojcc, 0x00)
                getmap = [
                    (hex(e), epc2str(eojgc, eojcc, e))
                    for e in state["instances"][eojgc][eojcc][instance][ENL_GETMAP]
                ]
                setmap = [
                    (hex(e), epc2str(eojgc, eojcc, e))
                    for e in state["instances"][eojgc][eojcc][instance][ENL_SETMAP]
                ]

                await server.getIdentificationInformation(
                    target, eojgc, eojcc, instance
                )
                uid = state["instances"][eojgc][eojcc][instance][ENL_UID]
                manufacturer = state["instances"][eojgc][eojcc][instance][
                    ENL_MANUFACTURER
                ]
                instance_list.append(
                    {
                        "host": target,
                        "group": (hex(eojgc), EOJX_GROUP[eojgc]),
                        "class": (hex(eojcc), EOJX_CLASS[eojgc][eojcc]),
                        "instance": hex(instance),
                        "getmap": getmap,
                        "setmap": setmap,
                        "uid": uid,
                        "manufacturer": manufacturer,
                    }
                )

    #pprint(instance_list)

    pcs = Factory(pcs_IP, server, 2, 0xa5, 1)

    #0xd0 grid connected:
    #System interconnected(reverse power flow acceptable)=0x00
    #Independent = 0x01
    #interconnected(reverse power flow not acceptable)

    while True:
        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            res = await pcs.update([0xd0,0xe0,0xe3,0xe7,0xf5,0xf6])

            #NOTE: the official parameters stop updating when battery is in standby
            #only 0xf5 and 0xf6 continue updating at that time

            e0=ctypes.c_uint32(int(res[0xe0], 16)).value #0xE0: "Measured cumulative amount of electric energy (normal direction)",
            e3 = ctypes.c_uint32(int(res[0xe3], 16)).value #0xE3: "Measured cumulative amount of electric energy (reverse direction)",
            e7 = ctypes.c_int32(int(res[0xe7], 16)).value #0xE7: "Measured instantaneous amount of electricity",

            grid_connected_val=ctypes.c_int32(int(res[0xd0], 16)).value
            val1 = ctypes.c_int32(int(res[0xf5][0:8], 16)).value #household only power
            val2 = ctypes.c_int32(int(res[0xf5][8:16], 16)).value #purchasing from grid

            normal_dir=ctypes.c_uint32(int(res[0xf6][0:8], 16)).value  #cumulative from the grid
            reverse_dir = ctypes.c_uint32(int(res[0xf6][8:16], 16)).value #cumulative sold to the grid

            grid_connected="true"
            if grid_connected_val==1: #independent mode
                grid_connected = "false"
            try:
                mqttClient.publish(sensor_data_path + "grid_connected", grid_connected, qos=0)
                mqttClient.publish(sensor_data_path + "Measured_cumulative_amount_of_electric_energy_normal_direction", e0, qos=0)
                mqttClient.publish(sensor_data_path + "Measured_cumulative_amount_of_electric_energy_reverse_direction", e3, qos=0)
                mqttClient.publish(sensor_data_path + "Measured_instantaneous_amount_of_electricity", e7, qos=0)

                mqttClient.publish(sensor_data_path + "home_energy_from_grid", -val1, qos=0)
                mqttClient.publish(sensor_data_path + "home_only_energy_consumption", val2, qos=0)
                mqttClient.publish(sensor_data_path + "cumulative_energy_normal_direction", normal_dir, qos=0)
                mqttClient.publish(sensor_data_path + "cumulative_energy_reverse_direction", reverse_dir, qos=0)


            except Exception as e:
                print(f"Failed to publish data. Retrying... Error: {e}")

            print(
            f"{current_time} 0xD0: {res[0xd0]} \t0xE0: {e0}Wh \t0xE3: {e3}Wh \t0xE7: {e7}W \t0xF5: {-val1}W {val2}W, \t0xF6: {normal_dir}Wh {reverse_dir}Wh")

        except Exception as err:
            print(f"Exception: {err}")
        time.sleep(pollingInterval)



if __name__ == "__main__":
    asyncio.run(main(sys.argv))
