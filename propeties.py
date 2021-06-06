import json

data = None
with open("propeties.json", "r") as read_file:
    data = json.load(read_file)
    read_file.close()

RUNNING_INSTANCES = data['amount_of_devices_running']

def get_instances():
    return RUNNING_INSTANCES

def add_instance():
    global RUNNING_INSTANCES
    RUNNING_INSTANCES += 1
    with open('propeties.json', 'w') as write_file:
        data['amount_of_devices_running'] = RUNNING_INSTANCES
        json.dump(data, write_file)
        write_file.close()

def delete_instance():
    global RUNNING_INSTANCES
    RUNNING_INSTANCES -= 1
    with open('propeties.json', 'w') as write_file:
        data['amount_of_devices_running'] = RUNNING_INSTANCES
        json.dump(data, write_file)
        write_file.close()

    

if __name__ == "__main__":
    print('propeties.py - Running tests.')


    '''
    delete_instance()

    print(RUNNING_INSTANCES)'''

else:
    print('propeties.py - IMPORTED')