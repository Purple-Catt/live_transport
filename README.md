# Amsterdam tram lines Real-Time
This code is part of a project which consists in a LEDs map of the public transport of Amsterdam that shows the real time position of all the vehicles in the city.

A Raspberry Pi is used to control the LEDs via I2C buses - the built-in one and a second virtual one at the moment - and GPIO expander MCP23017 that turn on the LEDs using BJT 2N2222A as electronic switch, to stick to the current limitation of the motherboard and the expander themselves.
The current version _Alpha 1.0.0_ it's not been tested on the aforementioned hardware yet, so I can't ensure it properly works as described.
