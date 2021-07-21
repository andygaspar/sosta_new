import pandas as pd
import numpy as np

from SeriesAnalysis import gf as granf
from SeriesAnalysis import new_entrant as ne
from SeriesAnalysis import find_series as fs
from SeriesAnalysis import db_voli as db
import multiprocessing
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}

# airports
df_airports = pd.read_csv("DataAirport/airports.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()
df_eu_airport = pd.read_csv("DataAirport/eu_airport.csv")
lev_12_airports = df_airports[(df_airports.level == 1) | (df_airports.level == 2)].airport.to_list()
all_airports = df_eu_airport.airport.to_list()


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


# df_eu_18
df_eu_18 = pd.read_csv("DataSummer/summer_2018.csv")

# df_eu_19
df_eu_19 = pd.read_csv("DataSummer/summer_2019.csv")

for week_day in range(7):
    print("********* ", week_day, " ************")

    df_eu_day_18 = df_eu_18[df_eu_18["week day"] == week_day].copy()
    date_num_18 = dict(zip(np.sort(df_eu_day_18.day.unique()), range(len(df_eu_day_18.day.unique()))))
    df_eu_day_18["day_num"] = df_eu_day_18.day.apply(lambda d: date_num_18[d])

    print(df_eu_day_18.airline.unique().shape[0], "airlines 18")

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

    print("parallel time:", time.time() - t)
    print("check 18 slot:", db_slot_18.shape[0] == db_slot_18.id.unique().shape[0])
    print("check 18 voli:", voli_18.shape[0] == voli_18.id.unique().shape[0])

    voli_18["time"] = voli_18.time.astype(int)
    voli_18["gf"] = [False for _ in range(voli_18.shape[0])]
    db_slot_18.to_csv("results/18/slot_2018_" + day_dict[week_day] + ".txt", sep="\t", index=False, index_label=False)
    voli_18.to_csv("results/18/voli_2018_" + day_dict[week_day] + ".txt", sep="\t", index=False, index_label=False)

    df_gf = granf.make_df_gf(db_slot_18)
    df_gf.to_csv("results/19/" + day_dict[week_day] + "/gf_" + day_dict[week_day] + ".txt", sep="\t", index=False,
                 index_label=False)

    df_eu_day_19 = df_eu_19[df_eu_19["week day"] == week_day].copy()
    date_num_19 = dict(zip(np.sort(df_eu_day_19.day.unique()), range(len(df_eu_day_19.day.unique()))))
    df_eu_day_19["day_num"] = df_eu_day_19.day.apply(lambda d: date_num_19[d])

    print(df_eu_day_19.airline.unique().shape[0], "airlines 19")

    parallel_input_19 = split_df(num_procs, df_eu_day_19, df_gf)

    t = time.time()
    pool = multiprocessing.Pool(num_procs)
    result = pool.map(find_slot_series, parallel_input_19)
    slots, voli = [item[0] for item in result], [item[1] for item in result]
    db_slot_19 = pd.concat(slots, ignore_index=True)
    voli_19 = pd.concat(voli, ignore_index=True)
    pool.close()
    pool.join()

    print("parallel time:", time.time() - t)
    print("check 19 slot:", db_slot_19.shape[0] == db_slot_19.id.unique().shape[0])
    print("check 19 voli:", voli_19.shape[0] == voli_19.id.unique().shape[0])

    columns = ["airport", "airline", "day"]
    new_entrant = pd.DataFrame(columns=columns)
    for airline in voli_19.airline.unique():
        v_air, gf_air = voli_19[voli_19.airline == airline], df_gf[df_gf.airline == airline]
        new_entrant = pd.concat([new_entrant, ne.make_new_entrant(airline, v_air, gf_air, airport_list)],
                                ignore_index=True)

    new_entrant.to_csv("results/19/" + day_dict[week_day] + "/new_entrant_" + day_dict[week_day] + ".txt",
                       sep="\t", index=False, index_label=False)

    series_dict = dict(zip(db_slot_19.id, range(db_slot_19.id.shape[0])))
    db_slot_19.id = db_slot_19.id.apply(lambda fl: series_dict[fl])
    db_slot_19.matched = db_slot_19.matched.apply(lambda fl: series_dict[fl] if fl != "N" else "N")

    db_slot_19.to_csv("results/19/" + day_dict[week_day] + "/db_slot_" + day_dict[week_day] + ".txt",
                      sep="\t", index=False, index_label=False)

    id_dict = dict(zip(voli_19.id, range(voli_19.id.shape[0])))
    voli_19.id = voli_19.id.apply(lambda fl: id_dict[fl])
    voli_19.match = voli_19.match.apply(lambda fl: id_dict[fl] if fl != -1 else -1)
    voli_19.series = voli_19.series.apply(lambda fl: series_dict[fl])

    db_voli = db.make_db_voli(voli_19)
    db_voli.to_csv("results/19/" + day_dict[week_day] + "/db_voli_" + day_dict[week_day] + ".txt",
                   sep="\t", index=False, index_label=False)
