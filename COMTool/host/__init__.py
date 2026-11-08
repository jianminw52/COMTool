"""On-site Remote Debug Host.

The process owns at most one serial port. The local window and the MQTT
device loop are consumers of that owner; neither opens the port itself.
"""

__version__ = "0.1.0"
