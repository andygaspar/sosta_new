import pandas as pd
import numpy as np
from SeriesAnalysis import find_2019_series as f_19
from SeriesAnalysis import find_2018_series as f_18
from SeriesAnalysis import gf as granf
from SeriesAnalysis import new_entrant as ne

# aggiungere reference match
# aggiungere controllo gf
# aggiungere make df_db_slot


voli = pd.read_csv("ok/voli_2019.csv", sep="\t")
gf = pd.read_csv("ok/gf_test.csv", sep="\t")

v = voli[voli.airline == "RYR"]
g = gf[gf.airline == "RYR"]

voli.airport.unique().shape





pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

# airports
df_airports = pd.read_csv("DataAirport/airports.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()
df_eu_airport = pd.read_csv("DataAirport/eu_airport.csv")
all_airports = df_eu_airport.airport.to_list()

# new_ryr = ne.make_new_entrant("RYR", v, g, airport_list)

# df_eu_18
df_eu_18 = pd.read_csv("DataSummer/summer_2018.csv")

# df_eu_19
df_eu_19 = pd.read_csv("DataSummer/summer_2019.csv")





week_day = 1




df_eu_day = df_eu_18[df_eu_18["week day"] == week_day].copy()
date_num = dict(zip(np.sort(df_eu_day.day.unique()), range(len(df_eu_day.day.unique()))))
df_eu_day["day_num"] = df_eu_day.day.apply(lambda d: date_num[d])

columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
db_slot_18 = pd.DataFrame(columns=columns)

print(df_eu_day.airline.unique().shape[0], "airlines")

cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
                 "CSVT", "gf", "turnaround", "match", "callsign"]
voli_18 = pd.DataFrame(columns=cols_db_voli)

i = 0
for airline in df_eu_day.airline.unique():
    print(i, airline)
    df_airline = df_eu_day[df_eu_day.airline == airline]
    db_s, db_v = f_18.check_airline_series(airline, df_airline, airport_list, all_airports, week_day)
    db_slot_18 = pd.concat([db_slot_18, db_s], ignore_index=True)
    voli_18 = pd.concat([voli_18, db_v], ignore_index=True)
    i += 1


db_slot_18.to_csv("ok/slot_2018.csv", sep="\t", index=False, index_label=False)
voli_18.to_csv("ok/voli_2018.csv", sep="\t", index=False, index_label=False)
# db_slot.to_csv("SeriesAnalysis/data_eu/db_slot_test.csv", index_label=False, index=False)


df_gf = granf.make_df_gf(db_slot_18)
df_gf.to_csv("ok/gf_test.csv", sep="\t", index=False, index_label=False)





columns = ["airport", "airline", "day"]
new_entrant = pd.DataFrame(columns=columns)



df_eu_day = df_eu_19[df_eu_19["week day"] == week_day].copy()
date_num = dict(zip(np.sort(df_eu_day.day.unique()), range(len(df_eu_day.day.unique()))))
df_eu_day["day_num"] = df_eu_day.day.apply(lambda d: date_num[d])

columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
db_slot_19 = pd.DataFrame(columns=columns)

print(df_eu_day.airline.unique().shape[0], "airlines")

cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
                 "CSVT", "gf", "turnaround", "match", "callsign"]
voli_19 = pd.DataFrame(columns=cols_db_voli)

voli_2018 = pd.read_csv("ok/voli_2018.csv", sep="\t")
voli_2018["gf"] = [False for _ in range(voli_2018.shape[0])]

i = 0
for airline in df_eu_day.airline.unique():
    print(i, airline)
    df_airline, df_air_18 = df_eu_day[df_eu_day.airline == airline], voli_2018[voli_2018.airline == airline]
    db_s, db_v = f_19.check_airline_series(airline, df_airline, df_air_18, airport_list, all_airports, week_day)
    db_slot_19 = pd.concat([db_slot_19, db_s], ignore_index=True)
    voli_19 = pd.concat([voli_19, db_v], ignore_index=True)
    new_entrant = pd.concat([new_entrant, ne.make_new_entrant(airline, db_v, df_gf[df_gf.airline == airline],
                                                              airport_list)], ignore_index=True)
    i += 1

db_slot_19.to_csv("ok/slot_2019.csv", sep="\t", index=False, index_label=False)
voli_19.to_csv("ok/voli_2019.csv", sep="\t", index=False, index_label=False)

print("c")

df_eu_19[~df_eu_19.departure.isin(airport_list)]
voli_19.shape