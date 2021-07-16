import pandas as pd
import numpy as np

start_minute = np.array([0, 540, 900, 1140])
end_minute = np.array([539, 899, 1139, 1439])

class Day:
    def __init__(self, day):
        self.day = day
        self.slots = {0: 0, 1: 0, 2: 0, 3: 0}


class Gf:
    def __init__(self, airline, df_airline=None):
        self.airline = airline
        self.airports = {}
        if df_airline is not None:
            self.set_gf_from_df(df_airline)

    @staticmethod
    def get_interval(time):
        for t in range(3):
            if start_minute[t] <= time <= end_minute[t]:
                return t
        return 3

    def add_gf(self, airport, initial, final, time):
        if airport not in self.airports.keys():
            self.airports[airport] = [Day(d) for d in range(30)]
        for d in range(initial, final + 1):
            self.airports[airport][d].slots[self.get_interval(time)] += 1

    def get_df(self):
        airline, airports, starts, ends, slots, days = [], [], [], [], [], []
        for airport in self.airports.keys():
            for d in range(30):
                for i in range(4):
                    slot_num = self.airports[airport][d].slots[i]
                    if slot_num > 0:
                        airline.append(self.airline)
                        airports.append(airport)
                        starts.append(start_minute[i])
                        ends.append(end_minute[i])
                        slots.append(slot_num)
                        days.append(d)

        return pd.DataFrame({"airport": airports, "airline": airline, "start": starts, "end": ends,
                             "slots": slots,  "day_num": days})

    def set_gf_from_df(self, df_airline):
        for i in range(df_airline.shape[0]):
            line = df_airline.iloc[i]
            self.add_gf(line.A_ICAO, line.InitialDate, line.FinalDate, line.Time)
