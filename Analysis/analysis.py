import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Analysis import load_files as lf

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}

df_airports = pd.read_csv("DataAirport/airports.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()
df_eu_airport = pd.read_csv("DataAirport/eu_airport.csv")
lev_12_airports = df_airports[(df_airports.level == 1) | (df_airports.level == 2)].airport.to_list()
all_airports = df_eu_airport.airport.to_list()

voli_18, voli_19 = lf.load_voli()
slot_18, slot_19 = lf.load_slot()
gf, new = lf.load_gf_new_entrant()


def make_fl_analysis(voli, slot, year):
    value = "-1" if year == 2018 else -1
    match = voli[voli.match != value]
    voli = voli[voli.match == value]
    airports = ["TOTAL"] + airport_list
    fl = [voli.shape[0]] + [voli[voli.airport == airport].shape[0] for airport in airport_list]
    sl = [slot.shape[0]] + [slot[slot.A_ICAO == airport].shape[0] for airport in airport_list]

    matched = [match.shape[0]] + [match[match.airport == airport].shape[0] for airport in airport_list]

    return pd.DataFrame({"Airport": airports, "flights": fl, "matched": matched, "series": sl})

fl_18 = make_fl_analysis(voli_18, slot_18, 2018)



slot_18.shape
voli_18[voli_18.match == "-1"]
voli_18.shape

arip_18 = {}

df_eu_18 = pd.read_csv("DataSummer/summer_2018.csv")
df_eu_18[df_eu_18.arrival == "EGPD"]