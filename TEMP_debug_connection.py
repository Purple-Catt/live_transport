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

chip = MCP23017(i2c=i2c_1, address=0x20)
# Creating (output) pins instances
output_pins = dict()
for n in range(0, 16):
    output_pins[f'{n}'] = chip.get_pin(n)

# Set pins to output with initial value False
for pin in output_pins.keys():
    output_pins[pin].switch_to_output()

while True:
    if input('0 or 1: ') == '0':
        val = False

    else:
        val = True

    p = input('Which pin (0-15): ')
    output_pins[p].value = val
