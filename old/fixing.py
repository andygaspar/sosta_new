import pandas as pd
import numpy as np

db_v = pd.read_csv("../DBvoli_Friday.txt", sep="\t")

match = db_v[db_v.match != -1]
match = db_v.drop_duplicates(subset="match")
non_macth = db_v[db_v.match == -1]

db_voli = pd.concat([match, non_macth], ignore_index=True)
db_voli = db_voli.drop_duplicates()
db_voli_m = db_voli[db_voli.match != -1]
series_m = db_voli_m.series

db_voli_s = db_voli[db_voli.match == -1]
series_s = db_voli_s.series

id = db_voli.series.unique()
airline, A_ICAO, Time, InitialDate, FinalDate, matched = [], [], [], [], [], []
matched_arrivals = 0

for ser in id:
    db_ser = db_voli[db_voli.series == ser]
    airline.append(db_ser.airline.unique()[0])
    InitialDate.append(min(db_ser.day))
    FinalDate.append(max(db_ser.day))
    A_ICAO.append(db_ser.airport.unique()[0])
    match_ser = db_voli[db_voli.id == db_ser.match[0]].series[0] if db_ser.match[0] != -1 else -1
    matched.append(match_ser)

    mean = db_ser.time.mean()
    std = db_ser.time.std()

    if std < 200:
        pass