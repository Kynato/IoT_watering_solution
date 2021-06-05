INTERVAL = 5
MSG_TXT = '{{"pressure": {pressure},"power_state": {power_state}}}'
from agent import Agent
agent = Agent()

def main():
    agent = Agent()

    # OBTAIN CONNECTION KEY
    if D1_KEY != None:
        CONNECTION_STRING = D1_KEY
    else:
        print('Provide connection string in connection_strings.py file.')

    # TRY TO CONNECT
    try:
        client = iothub_client_init(CONNECTION_STRING)
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )

        # Start a thread to listen 
        device_method_thread = threading.Thread(target=device_method_listener, args=(client,))
        device_method_thread.daemon = True
        device_method_thread.start()

        while True:
            # Get agent propeties
            pw_st = 1
            if not agent.power_state:
                pw_st = 0

            press = agent.pressure

            # Build the message with simulated telemetry values.
            msg_txt_formatted = MSG_TXT.format(pressure=press, power_state=pw_st)
            message = Message(msg_txt_formatted)

            # Add a custom application property to the message.
            # An IoT hub can filter on these properties without access to the message body.
            if press > agent.pressure_limit:
                message.custom_properties["pressure_limit_exceeded"] = "true"
            else:
                message.custom_properties["pressure_limit_exceeded"] = "false"

            if agent.error_0:
                message.custom_properties["ERROR_0"] = True
            else:
                message.custom_properties["ERROR_0"] = False
            if agent.error_1:
                message.custom_properties["ERROR_1"] = "true"
            else:
                message.custom_properties["ERROR_1"] = "false"
            if agent.error_2:
                message.custom_properties["ERROR_2"] = "true"
            else:
                message.custom_properties["ERROR_2"] = "false"


            # Send the message.
            print( "Sending message: {}".format(message) )
            client.send_message(message)
            print( "Message sent" )

            # Sleep
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

def device_method_listener(device_client):
    global INTERVAL
    while True:
        method_request = device_client.receive_method_request()
        print (
            "\nMethod callback called with:\nmethodName = {method_name}\npayload = {payload}".format(
                method_name=method_request.name,
                payload=method_request.payload
            )
        )
        if method_request.name == "set_pressure":
            try:
                print(float(method_request.payload))
                agent.set_pressure(method_request.payload)

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
    import azure
    import json
    import time
    import threading
    from connection_strings import D1_KEY
    

    main()

else:
    print('main.py - Imported externally. Weird...')