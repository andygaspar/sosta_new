import pandas as pd
from airport import Airport
from flight import Flight
import numpy as np
import datetime
from os import walk


"""
Extract series from origin destination files
"""

from calendar import monthrange
month = [0]
for i in range(1, 12):
    val = month[-1]
    month.append(monthrange(2018, i)[1]+month[-1])


airports = pd.read_csv("data/airports.csv", index_col=None).drop(columns="Unnamed: 0")
print(airports)

_, _, filenames = next(walk("Coppie_OD/not_moved"))

num = 0
num_series = 0
flights_found = []
df = pd.read_csv("Coppie_OD/not_moved/"+filenames[0], sep="\t")
final_df = pd.DataFrame(columns=list(df.columns))
print(df)

for file in filenames:
    if file[:4] in airports["airport"].tolist() or file[5:9] in airports["airport"].tolist():
        num += 1
        df = pd.read_csv("Coppie_OD/not_moved/"+file, sep="\t", index_col=False)
        g = df.sort_values(by="date", ignore_index=True)
        p = g["date"].apply(lambda d: month[int(d[5:7])] + int(d[8:10]))
        g["date num"] = p.astype('int32')
        print(num, num_series)

        for i in range(7):
            hhh = g[((g["date num"] - min(p)-i) % 7 == 0) & (g["date num"] -  min(p)-i >= 0)]
            if hhh.shape[0] > 0:
                for cs in hhh["callsign"]:
                    same_call = hhh[hhh["callsign"] == cs].copy()
                    if same_call.shape[0] > 4:
                        if cs not in flights_found:
                            same_call["series"] = num_series
                            final_df = pd.concat([final_df, same_call], ignore_index=True)
                            flights_found.append(cs)
                            num_series += 1

final_df = final_df.drop(columns=["Unnamed: 0", "trajectory"])
final_df["date num"] = final_df["date num"].astype(int)
final_df["series"] = final_df["series"].astype(int)
final_df.to_csv("series_from_od.csv", index=False)

print(len(flights_found))






#
