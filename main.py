from connection_strings import connection_string_1

if connection_string_1 != None:
    conn_string = connection_string_1
else:
    print('Provide connection string in connection_strings.py file.')


if __name__ == "__main__":

    # Announcement
    print('main.py - Running')

    # Imports
    import asyncio
    import os
    import uuid
    import json
    import random

    print(conn_string)




else:
    print('main.py - Imported externally. Weird...')