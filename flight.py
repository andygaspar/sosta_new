
class Flight:

    def __init__(self, icao, callsign, uuid, origin, destination):
        self.icao = icao
        self.callSign = callsign
        self.uuid = uuid
        self.origin = origin
        self.destination = destination

    def __str__(self):
        return self.icao + " " + self.callSign + " " + self.uuid + " " + self.origin + " " + self.destination

    def __repr__(self):
        return self.icao + " " + self.callSign + " " + self.uuid + " " + self.origin + " " + self.destination
