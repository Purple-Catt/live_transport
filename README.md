# Amsterdam tram lines Real-Time
This code is part of a project which consists of a public transport map of Amsterdam that shows the real time position of all the vehicles in the city using LEDs.

I'm using open data with [CC0 disclaimer](https://creativecommons.org/publicdomain/zero/1.0/) distributed by Stichting OpenGeo as part of the NDOV Loket project and received using ZeroMQ with a pub/sub type of socket connection (more information on the official [website](https://ndovloket.nl/index.html)). 

I use a Raspberry Pi (3B+) to receive data and control the LEDs. To increase the number of outputs, GPIO expanders MCP23017 have been used. This circuit works with I2C bus to add 16 GPIO to the main controller and, thanks to 3-pin address, it's possible to connect up to 8 of them to the same bus, reaching 128 GPIO per I2C bus. A virtual I2C bus has been added to the RasPi to be able to reach up to 256 outputs but, in case higher speed is needed, a multiplex can be used as well. LEDs are switched using BJT 2N2222A, to stick to the current limitation of the motherboard and the expanders themselves (further details on the MCP23017 [datasheet](https://ww1.microchip.com/downloads/en/devicedoc/20001952c.pdf)).

The current version _Alpha 1.0.0_ has not been tested on the aforementioned hardware yet, so I can't ensure it properly works as described. Relevant updates will come soon.
