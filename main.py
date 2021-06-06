INTERVAL = 5
MSG_TXT = '{{"pressure": {pressure},"power_state": {power_state}}}'

from agent import Agent
the_device = Agent()

def main():

    # OBTAIN CONNECTION KEY
    if D1_KEY != None:
        CONNECTION_STRING = D1_KEY
    else:
        print('Provide connection string in connection_strings.py file.')

    # TRY TO CONNECT
    try:
        client = iothub_client_init(CONNECTION_STRING)
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )

        twin_update_listener_thread = threading.Thread(target=twin_update_listener, args=(client,))
        twin_update_listener_thread.daemon = True
        twin_update_listener_thread.start()

        

        # Start a thread to listen 
        device_method_thread = threading.Thread(target=device_method_listener, args=(client,))
        device_method_thread.daemon = True
        device_method_thread.start()

        while True:
            # Get the_device propeties
            pw_st = 1
            if not the_device.power_state:
                pw_st = 0

            press = the_device.get_pressure()

            # Build the message with simulated telemetry values.
            msg_txt_formatted = MSG_TXT.format(pressure=press, power_state=pw_st)
            message = Message(msg_txt_formatted)

            # Add a custom application property to the message.
            # An IoT hub can filter on these properties without access to the message body.
            if press > the_device.pressure_limit:
                message.custom_properties["pressure_limit_exceeded"] = True
            else:
                message.custom_properties["pressure_limit_exceeded"] = False

            if the_device.error_0:
                message.custom_properties["ERROR_0"] = True
            else:
                message.custom_properties["ERROR_0"] = False
            if the_device.error_1:
                message.custom_properties["ERROR_1"] = True
            else:
                message.custom_properties["ERROR_1"] = False
            if the_device.error_2:
                message.custom_properties["ERROR_2"] = True
            else:
                message.custom_properties["ERROR_2"] = False


            # Send the message.
            print( "Sending message: {}".format(message) )
            client.send_message(message)
            print( "Message sent" )

            # Sleep
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

def twin_update_listener(client):
    '''
    Twin patch received:
        {'power_level': 1223333, '$version': 7}
    '''
    while True:
        patch = client.receive_twin_desired_properties_patch()  # blocking call
        print("Twin patch received:")
        print(patch)
        
        the_device.set_pressure(patch['pressure'])
        the_device.power_state = patch['power_state']
    


def twin_send_report(client):
    # Send reported 
    print ( "Sending data as reported property..." )
    reported_patch = {"pressure": the_device.get_pressure(), "power_state": the_device.power_state}
    client.patch_twin_reported_properties(reported_patch)
    print ( "Reported properties updated" )

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
                the_device.raise_alarm(error_nr = int((method_request.payload)))
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

def iothub_client_init(CONNECTION_STRING):
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client


if __name__ == "__main__":

    # Announcement
    print('main.py - Running')

    # Imports
    import asyncio
    from azure.iot.device import IoTHubDeviceClient, Message, MethodResponse
    from azure.iot.hub import IoTHubRegistryManager
    from azure.iot.hub.models import Twin, TwinProperties, QuerySpecification, QueryResult
    import azure
    import json
    import time
    import threading
    from connection_strings import D1_KEY, D1_NAME, HUB_KEY
    

    main()

else:
    print('main.py - Imported externally. Weird...')