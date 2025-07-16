from gzip import GzipFile
from io import BytesIO
from os import system, name
from datetime import datetime as dt, timedelta
from time import sleep
import zmq
from ctx import ctx_decode
import pandas as pd
from adafruit_mcp230xx.mcp23017 import MCP23017
from adafruit_extended_bus import ExtendedI2C

first = True
lines = ['1', '2', '17']
start = dt.strptime("06:00", "%H:%M").time()
end = dt.strptime("23:59", "%H:%M").time()
qc_pin_map = dict()
for line in lines:
    qc_pin_map[line] = pd.read_csv(f'quaycodes_encoding\\{line}.csv', header=0)

# Activating connection to receiving data
context = zmq.Context()
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://pubsub.besteffort.ndovloket.nl:7817")
subscriber.setsockopt_string(zmq.SUBSCRIBE, "/GOVI/KV8")


def clear():
    if name == 'nt':
        x = system('cls')

    else:
        x = system('clear')


# Initializing the I2C buses
i2c_1 = ExtendedI2C(2)
i2c_2 = ExtendedI2C(3)

# Creating chips instances
chips = {
    'mcp_1': MCP23017(i2c=i2c_1, address=0x20),
    'mcp_2': MCP23017(i2c=i2c_1, address=0x21),
    'mcp_3': MCP23017(i2c=i2c_1, address=0x22),
    'mcp_4': MCP23017(i2c=i2c_1, address=0x23),
    'mcp_5': MCP23017(i2c=i2c_1, address=0x24),
    'mcp_6': MCP23017(i2c=i2c_1, address=0x25),
    'mcp_7': MCP23017(i2c=i2c_1, address=0x26),
    'mcp_8': MCP23017(i2c=i2c_1, address=0x27),
    'mcp_9': MCP23017(i2c=i2c_2, address=0x20)
}
# Creating (output) pins instances
output_pins = dict()
for key in chips.keys():
    for n in range(0, 16):
        output_pins[f'pin_{(int(key.removeprefix("mcp_")) * 100) + n}'] = chips[key].get_pin(n)

# Set pins to output with initial value False
for pin in output_pins.keys():
    output_pins[pin].switch_to_output()

# Run only between 6:00am and 11:59pm, also to avoid timestamps' problems in data after midnight
while True:
    if start < dt.now().time() < end:
        multipart = subscriber.recv_multipart()
        address = multipart[0]
        contents = b''.join(multipart[1:])
        contents = GzipFile('', 'r', 0, BytesIO(contents)).read()
        str_contents = contents.decode('utf-8')  # All data encoded in CTX
        splitted_content = str_contents.split('\n')
        if splitted_content[3].startswith('GVB'):
            if splitted_content[1].startswith(r'\TDATEDPASSTIME'):
                # Keep a dataframe updated with real time data of vehicles driving predefined lines managed by GVB
                if first:
                    df = ctx_decode(str_contents)
                    df.drop(index=df.loc[df['RecordedArrivalTime'] == r'\0'].index,
                            inplace=True)
                    df = df[df['LinePublicNumber'].isin(lines)]

                    if df.duplicated(subset=['VehicleNumber']).any():
                        df.sort_values(by='RecordedArrivalTime',
                                       inplace=True,
                                       kind='mergesort',
                                       key=lambda x: pd.to_datetime(x))
                        df.sort_values(by='VehicleNumber',
                                       inplace=True,
                                       kind='mergesort')
                        df.drop_duplicates(subset=['VehicleNumber'],
                                           keep='last',
                                           inplace=True)

                    if not df.empty:
                        df.set_index('VehicleNumber', inplace=True)
                        first = False

                else:
                    temp = ctx_decode(str_contents)
                    temp.drop(index=temp.loc[temp['RecordedArrivalTime'] == r'\0'].index,
                              inplace=True)
                    temp = temp[temp['LinePublicNumber'].isin(lines)]

                    if temp.duplicated(subset=['VehicleNumber']).any():
                        temp.sort_values(by='RecordedArrivalTime',
                                         inplace=True,
                                         kind='mergesort',
                                         key=lambda x: pd.to_datetime(x))
                        temp.sort_values(by='VehicleNumber',
                                         inplace=True,
                                         kind='mergesort')
                        temp.drop_duplicates(subset=['VehicleNumber'],
                                             keep='last',
                                             inplace=True)

                    if not temp.empty:
                        temp.set_index('VehicleNumber', inplace=True)
                        df.update(temp)

                        df = pd.concat([df, temp[~temp.index.isin(df.index)]])

                        # To avoid the leds flickering, all of them will be turned off and then on again using
                        # a static approach instead of a dynamic one, also speeding up the code.
                        for pin in output_pins.keys():
                            output_pins[pin].value = False

                        for idx in df.index:
                            line_map = qc_pin_map[df.loc[idx, 'LinePublicNumber']]
                            direction = df.loc[idx, 'LineDirection']
                            quaycode = df.loc[idx, 'QuayCode']
                            line_stop = line_map.loc[line_map[f'quaycode_{direction}'] == quaycode]

                            if df.loc[idx, 'RecordedDepartureTime'] == r'\0':
                                output_pins[line_stop['pin_stop'][0]].value = True

                            else:
                                output_pins[line_stop[f'pin_pass_{direction}'][0]].value = True

                            # Drop vehicles that haven't been updated for more than 10 mins
                            if dt.now() - dt.combine(dt.today().date(),
                                                     dt.strptime(df.loc[idx, 'RecordedArrivalTime'],
                                                                 '%H:%M:%S').time()) > timedelta(minutes=10):
                                output_pins[line_stop['pin_stop'][0]].value = False
                                df.drop(index=idx, inplace=True)

                        clear()
                        print(df.loc[:, ['LinePublicNumber', 'RecordedArrivalTime', 'RecordedDepartureTime',
                                         'QuayCode', 'DestinationName', 'LineDirection', 'TripStopStatus',
                                         'JourneyStopType']])

            else:
                print(str_contents)

    else:
        # Turn off all LEDs outside working hours
        for pin in output_pins.keys():
            output_pins[pin].value = False

        sleep(30)
