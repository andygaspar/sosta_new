

class Airport:

    def __init__(self, name, capacity = None):
        self.name = name
        self.capacity = capacity
        self.flights = []

    def add_flight(self, flight):
        self.flights.append(flight)