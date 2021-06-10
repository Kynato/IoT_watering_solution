INTERVAL = 10
MSG_TXT = '{{"device_id": {device_id}, "pressure": {pressure}, "power_state": {power_state}}}'
ERROR_TXT = '{{"device_id": {device_id}, "ERROR_0": {ERROR_0}, "ERROR_1": {ERROR_1}, "ERROR_2": {ERROR_2}}}'

import logging
from agent import Agent
the_device = Agent()

def main():
    # Check for devices online
    DEVICES_ONLINE = propeties.get_instances()
    DEVICE_CONNECTION_STRING = DEVICE_KEYS[DEVICES_ONLINE]

    # OBTAIN CONNECTION KEY
    if DEVICE_CONNECTION_STRING != None:
        CONNECTION_STRING = DEVICE_CONNECTION_STRING
        the_device.set_device_id(DEVICES_ONLINE)
    else:
        print('Provide connection string in connection_strings.py file.')
        exit

    # Connect, start threads and send messages
    try:
        # Set up the connection to device
        client = iothub_client_init(CONNECTION_STRING)

        

        
        loop = asyncio.get_event_loop()
        
        
        # Announce and increment devices online
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
        propeties.add_instance()

        # Send initial report in case something has changed
        twin_send_report(client)

        # Start a thread to listen to DeviceTwin Desired updates
        twin_update_listener_thread = threading.Thread(target=twin_update_listener, args=(client,))
        twin_update_listener_thread.daemon = True
        twin_update_listener_thread.start()


        # Start a thread to listen to Direct Methods
        device_method_thread = threading.Thread(target=device_method_listener, args=(client,))
        device_method_thread.daemon = True
        device_method_thread.start()

        
        # Message loop
        while True:

            # Get the_device propeties
            pw_st = 1
            if not the_device.power_state:
                pw_st = 0
            press = the_device.get_pressure()

            loop.run_until_complete(send_event())
            # Build the message with simulated telemetry values.
            msg_txt_formatted = MSG_TXT.format(device_id = the_device.device_id,pressure=press, power_state=pw_st)
            message = Message(msg_txt_formatted)
            message.custom_properties['level'] = 'storage'

            if the_device.get_alarm_state():
                errors = the_device.get_errors_int()
                error_txt_formatted = ERROR_TXT.format(device_id = the_device.device_id, ERROR_0=int(errors[0]), ERROR_1=int(errors[1]), ERROR_2=int(errors[2]))
                error_message = Message(error_txt_formatted)
                error_message.custom_properties['level'] = 'critical'

                # Send the message.
                print( "Sending ERROR_MESSAGE: {}".format(error_message) )
                client.send_message(error_message)

            # Send the message.
            print( "Sending message: {}".format(message) )
            client.send_message(message)

            # Sleep
            time.sleep(INTERVAL)

    # Stop device and delete it from online
    except KeyboardInterrupt:
        print ( "Agent instance - STOPPED..." )
        
async def send_event():
    # CHANGE BEFORE GIT PUSH
    producer = EventHubProducerClient.from_connection_string(conn_str = EVENT_HUB_KEY, eventhub_name="thehub")

    try:
        event_data_batch = await producer.create_batch()
        while True:
            try:
                event_data_batch.add(EventData('Message inside EventBatchData'))
            except ValueError:
                # The EventDataBatch object reaches its max_size.
                # You can send the full EventDataBatch object and create a new one here.
                break
        await producer.send_batch(event_data_batch)
    finally:
        # Close down the producer handler.
        print('Should be sent.')
        await producer.close()

'''
    async with producer:
        event_data_batch = await producer.create_batch()

        while True:
            try:
                event_data_batch.add(EventData('ERROR RAISED'))
            except ValueError:
                print( 'Error sending the EVENT...')
                break
        
        await producer.send_batch(event_data_batch)
        producer.send_message

        print('Event sent.')
        print(event_data_batch.body_as_json())
        await producer.close()'''

# Device Twin Listener waiting for Desired propeties
def twin_update_listener(client):

    while True:
        patch = client.receive_twin_desired_properties_patch()  # blocking call
        print("Twin patch received:")
        print(patch)
        
        # patch is a dictionary type
        the_device.set_pressure( float(patch['pressure']) )
        #the_device.power_state = patch['power_state']
        twin_send_report(client)

    
    

# Sends data to Device Twin as Reported
def twin_send_report(client):
    print ( "Sending data as reported property..." )

    # Prepare data to send
    reported_patch = {"pressure": the_device.pressure, "power_state": the_device.power_state, "ERROR_0": the_device.error_0, "ERROR_1": the_device.error_1, "ERROR_2": the_device.error_2}
    # Send the data
    client.patch_twin_reported_properties(reported_patch)

    # Announce it
    print ( "Reported properties updated" )

# Listens to Direct Method calls and processes tem
def device_method_listener(device_client):
    global the_device
    while True:
        method_request = device_client.receive_method_request()
        print (
            "\nMethod callback called with:\nmethodName = {method_name}\npayload = {payload}".format(
                method_name=method_request.name,
                payload=method_request.payload
            )
        )
        # SET_PRESSURE
        if method_request.name == "set_pressure":
            try:
                the_device.set_pressure(desired_pressure = float(method_request.payload))
                twin_send_report(device_client)

            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
            else:
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404

        # PUMP_SWITCH
        if method_request.name == "pump_switch":
            try:
                the_device.pump_switch()
                twin_send_report(device_client)
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
            else:
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404
        
        # RAISE_ERROR
        if method_request.name == "raise_error":
            try:
                the_device.raise_error(error_nr = int((method_request.payload)))
                twin_send_report(device_client)
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
            else:
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404

        # ALARM_RESET
        if method_request.name == "alarm_reset":
            try:
                the_device.alarm_reset()
                twin_send_report(device_client)
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
            else:
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404

        method_response = MethodResponse(method_request.request_id, response_status, payload=response_payload)
        device_client.send_method_response(method_response)

# Creates the connection to the device
def iothub_client_init(CONNECTION_STRING):
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client

# Get number of devices in the hub
def get_amount_of_devices():
    # Create connection to the hub
    iot_hub_manager = IoTHubRegistryManager(HUB_KEY)
    # Get list of the devices in the hub
    list_of_devices = iot_hub_manager.get_devices()
    # Count the devices on the list
    amount_of_devices = len(list_of_devices)

    # Print it on the screen and return
    print('Number of devices in the hub: ' + str( amount_of_devices ) )
    return amount_of_devices

def decrement_online_devices(sig):
    propeties.delete_instance()
    print('Decrementing online devices')
    time.sleep(1)

if __name__ == "__main__":

    # Announcement
    print('main.py - Running')

    # Imports
    from azure.iot.device import IoTHubDeviceClient, Message, MethodResponse
    from azure.iot.hub import IoTHubRegistryManager
    from azure.eventhub.aio import EventHubProducerClient
    from azure.eventhub import EventData
    from azure.iot.hub.models import Twin, TwinProperties, QuerySpecification, QueryResult
    import azure
    import asyncio
    import time
    import threading
    import propeties
    import win32api
    from connection_strings import HUB_KEY, DEVICE_KEYS, EVENT_HUB_KEY

    win32api.SetConsoleCtrlHandler(decrement_online_devices, True)

    main()

else:
    print('main.py - Imported externally. Weird...')