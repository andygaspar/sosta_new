import pandas as pd
import numpy as np
import psutil as psutil

from SeriesAnalysis import find_2019_series as f_19
from SeriesAnalysis import find_2018_series as f_18
from SeriesAnalysis import gf as granf
from SeriesAnalysis import new_entrant as ne
from SeriesAnalysis import find_series as fs
import multiprocessing
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)


# voli = pd.read_csv("ok/voli_2019.csv", sep="\t")
# gf = pd.read_csv("ok/gf_test.csv", sep="\t")


def split_df(num_procs, df, df_g=None):
    airlines_ordered = df.value_counts("airline").index
    air_partition = {}
    for i in range(num_procs):
        air_partition[i] = []

    air_index = 0
    while air_index < airlines_ordered.shape[0]:
        air_partition[air_index % num_procs].append(airlines_ordered[air_index])
        air_index += 1
    if df_g is None:
        return tuple([(df[df.airline.isin(air_partition[key])], None) for key in air_partition])
    else:
        return tuple([(df[df.airline.isin(air_partition[key])],
                       df_g[df_g.airline.isin(air_partition[key])]) for key in air_partition])


def find_slot_series(tup):
    df_day, df_gf = tup
    week_day = df_day["week day"].unique()[0]
    columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
    db_slot_18 = pd.DataFrame(columns=columns)
    cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
                    "CSVT", "gf", "turnaround", "match", "callsign"]
    voli_18 = pd.DataFrame(columns=cols_db_voli)
    # psutil.Process().cpu_num()

    for airline in df_day.airline.unique():
        df_airline = df_day[df_day.airline == airline]
        db_s, db_v = fs.check_airline_series(airline, df_airline, airport_list, lev_12_airports, all_airports, week_day)
        db_slot_18 = pd.concat([db_slot_18, db_s], ignore_index=True)
        voli_18 = pd.concat([voli_18, db_v], ignore_index=True)

    return db_slot_18, voli_18






# airports
df_airports = pd.read_csv("DataAirport/airports.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()
df_eu_airport = pd.read_csv("DataAirport/eu_airport.csv")
lev_12_airports = df_airports[(df_airports.level == 1) | (df_airports.level == 2)].airport.to_list()
all_airports = df_eu_airport.airport.to_list()


# df_eu_18
df_eu_18 = pd.read_csv("DataSummer/summer_2018.csv")

# df_eu_19
df_eu_19 = pd.read_csv("DataSummer/summer_2019.csv")





week_day = 1




df_eu_day_18 = df_eu_18[df_eu_18["week day"] == week_day].copy()
date_num_18 = dict(zip(np.sort(df_eu_day_18.day.unique()), range(len(df_eu_day_18.day.unique()))))
df_eu_day_18["day_num"] = df_eu_day_18.day.apply(lambda d: date_num_18[d])

# columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
# db_slot_18 = pd.DataFrame(columns=columns)

print(df_eu_day_18.airline.unique().shape[0], "airlines")

# cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
#                  "CSVT", "gf", "turnaround", "match", "callsign"]
# voli_18 = pd.DataFrame(columns=cols_db_voli)




t = time.time()
num_procs = multiprocessing.cpu_count()

parallel_input_18 = split_df(num_procs, df_eu_day_18)



pool = multiprocessing.Pool(num_procs)
result = pool.map(find_slot_series, parallel_input_18)
slots, voli = [item[0] for item in result], [item[1] for item in result]
db_slot_18 = pd.concat(slots, ignore_index=True)
voli_18 = pd.concat(voli, ignore_index=True)
pool.close()
pool.join()

print("parallel time:", time.time()-t)






# t = time.time()
#
# i = 0
# for airline in df_eu_day.airline.unique():
#     df_airline = df_eu_day[df_eu_day.airline == airline]
#     db_s, db_v = fs.check_airline_series(airline, df_airline, airport_list, all_airports, week_day)
#     db_slot_18 = pd.concat([db_slot_18, db_s], ignore_index=True)
#     voli_18 = pd.concat([voli_18, db_v], ignore_index=True)
#     i += 1
#
# print("time: ", time.time() - t)




voli_18["time"] = voli_18.time.astype(int)
voli_18["gf"] = [False for _ in range(voli_18.shape[0])]
# db_slot_18.to_csv("ok/slot_2018.csv", sep="\t", index=False, index_label=False)
# voli_18.to_csv("ok/voli_2018.csv", sep="\t", index=False, index_label=False)
# db_slot.to_csv("SeriesAnalysis/data_eu/db_slot_test.csv", index_label=False, index=False)


df_gf = granf.make_df_gf(db_slot_18)
# df_gf.to_csv("ok/gf_test.csv", sep="\t", index=False, index_label=False)




columns = ["airport", "airline", "day"]
new_entrant = pd.DataFrame(columns=columns)


df_eu_day_19 = df_eu_19[df_eu_19["week day"] == week_day].copy()
date_num_19 = dict(zip(np.sort(df_eu_day_19.day.unique()), range(len(df_eu_day_19.day.unique()))))
df_eu_day_19["day_num"] = df_eu_day_19.day.apply(lambda d: date_num_19[d])

# columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
# db_slot_19 = pd.DataFrame(columns=columns)

print(df_eu_day_19.airline.unique().shape[0], "airlines")

# cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
#                  "CSVT", "gf", "turnaround", "match", "callsign"]
# voli_19 = pd.DataFrame(columns=cols_db_voli)


parallel_input_19 = split_df(num_procs, df_eu_day_19, df_gf)

t = time.time()
pool = multiprocessing.Pool(num_procs)
result = pool.map(find_slot_series, parallel_input_19)
slots, voli = [item[0] for item in result], [item[1] for item in result]
db_slot_19 = pd.concat(slots, ignore_index=True)
voli_19 = pd.concat(voli, ignore_index=True)
pool.close()
pool.join()

print("parallel time:", time.time()-t)


# t = time.time()
#
# for airline in df_eu_day.airline.unique():
#     df_airline, df_air_18 = df_eu_day[df_eu_day.airline == airline], voli_18[voli_18.airline == airline]
#     db_s, db_v = fs.check_airline_series(airline, df_airline, airport_list, all_airports, week_day, df_air_18)
#     db_slot_19 = pd.concat([db_slot_19, db_s], ignore_index=True)
#     voli_19 = pd.concat([voli_19, db_v], ignore_index=True)
#     new_entrant = pd.concat([new_entrant, ne.make_new_entrant(airline, db_v, df_gf[df_gf.airline == airline],
#                                                               airport_list)], ignore_index=True)
#
# print("time:", time.time()-t)

# db_slot_19.to_csv("ok/slot_2019.csv", sep="\t", index=False, index_label=False)
# voli_19.to_csv("ok/voli_2019.csv", sep="\t", index=False, index_label=False)

print("c")



"""
slot 18 = 17517
slot 19 = 19947

voli 18 = 386560
voli 19 = 449990
"""


# time 447.6136031150818