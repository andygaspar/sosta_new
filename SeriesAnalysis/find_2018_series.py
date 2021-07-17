import pandas as pd
import numpy as np
from SeriesAnalysis.gf import Gf

# aggiungere reference match
# aggiungere controllo gf
# aggiungere make df_db_slot

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

# airports
df_airports = pd.read_csv("DataRefactor/airports.csv")
df_eu_airport = pd.read_csv("eu_airport.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()

# df_eu_clean
df_eu = pd.read_csv("DataSummer/summer_2018.csv")


# df_eu = ec.read_and_set_df_eu(2019)
# df_eu = pd.read_csv("SeriesAnalysis/data_eu/europe_cleaned.csv")





def approx_time(t):
    time_approximation = int(t / 10) * 10 if (t // 5) % 2 == 0 else int(t / 10) * 10 + 5
    return time_approximation


def check_mean(df_call: pd.DataFrame, is_departure: bool, tol: int, max_occurrence: float, min_series_len: int):
    dep_arr = "dep_min" if is_departure else "arr_min"
    airport_dep_arr = "departure" if is_departure else "arrival"

    airport = df_call[airport_dep_arr].mode().values[0]
    df_call_airport = df_call[df_call[airport_dep_arr] == airport]

    if df_call_airport.shape[0] >= min_series_len:
        mean = df_call_airport[dep_arr].mean()
        if df_call_airport[(df_call_airport[dep_arr] <= mean + tol)
                           & (df_call_airport[dep_arr] >= mean - tol)].shape[0] \
                >= df_call_airport.shape[0] * max_occurrence:
            df_call_airp_filtered = df_call_airport[(df_call_airport[dep_arr] <= mean + 400)
                                                    & (df_call_airport[dep_arr] >= mean - 400)]
            mean = df_call_airp_filtered[dep_arr].mean()

            days = df_call_airp_filtered.sort_values("day_num").day_num.to_list()
            init_day, final_day = days[0], days[-1]

            db_voli = df_call_airport[(df_call_airport.day_num >= init_day) & (df_call_airport.day_num <= final_day)]

            return True, approx_time(mean), init_day, final_day, db_voli
        else:
            return False, None, None, None, None

    return False, None, None, None, None


def make_df_voli(is_departure: bool, voli: pd.DataFrame, airport, series, id_fls, match, turn):
    dep_arr = "dep_min" if is_departure else "arr_min"
    flow_ = "D" if is_departure else "A"
    flow = [flow_ for _ in range(voli.shape[0])]
    airp = [airport for _ in range(voli.shape[0])]
    gf = ["N" for _ in range(voli.shape[0])]
    ser = [series for _ in range(voli.shape[0])]
    turns = [turn for _ in range(voli.shape[0])]
    csvt = ["J" for _ in range(voli.shape[0])]
    if match is None:
        match = [-1 for _ in range(voli.shape[0])]
    to_concat = pd.DataFrame({"id": id_fls, "airline": voli.airline.copy(deep=True), "flow": flow,
                              "airport": airp, "icao24": voli.icao24.copy(deep=True),
                              "day": voli.day_num,
                              "time": voli[dep_arr], "series": ser, "CSVT": csvt, "gf": gf,
                              "turnaround": turns, "match": match, "callsign": voli.callsign})
    return to_concat


def check_airline_series(airline, df_airline, airport_list, week_day):
    tol, max_occurrence, min_series_len = 30, 0.7, 5

    columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
    db_slot = pd.DataFrame(columns=columns)


    for callsign in df_airline.callsign.unique():
        df_call = df_airline[df_airline.callsign == callsign]

        # check multiple callsign per day
        if df_call.shape[0] >= min_series_len:
            if df_call.day.unique().shape[0] != df_call.shape[0]:
                df_call = df_call.drop_duplicates("day")

            departure, arrival = df_call.departure.mode().values[0], df_call.arrival.mode().values[0]

            is_departure, is_arrival = departure in airport_list, arrival in airport_list

            if is_departure:

                found_dep_series, mean_departure, init_day, final_day, voli \
                    = check_mean(df_call, True, tol, max_occurrence, min_series_len)

                if found_dep_series:
                    id_departure = airline + departure + arrival + callsign + str(week_day)
                    to_append = \
                        [id_departure] + [airline] + [departure] + [mean_departure] + [init_day] + \
                        [final_day] + ["N"]
                    db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

                    if is_arrival:
                        mean_arrival = approx_time(voli.arr_min.mean())
                        id_arrival = airline + arrival + departure + callsign + str(week_day)
                        matched = "N"
                        if mean_departure < mean_arrival:
                            matched = id_departure
                        to_append = \
                            [id_arrival] + [airline] + [arrival] + [mean_arrival] \
                            + [init_day] + [final_day] + [matched]
                        db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)


            # check series arrival
            if is_arrival and not is_departure:

                found_arr_series, mean_arrival, init_day, final_day, voli \
                    = check_mean(df_call, False, tol, max_occurrence, min_series_len)

                if found_arr_series:
                    id_arrival = airline + arrival + departure + callsign + str(week_day)
                    to_append = [id_arrival] + [airline] + [arrival] + [mean_arrival] + [init_day] + [final_day] + ["N"]
                    db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)


    return db_slot

#21961

# 19437
# 3634
week_day = 1

df_eu_day = df_eu[df_eu["week day"] == week_day].copy()
date_num = dict(zip(np.sort(df_eu_day.day.unique()), range(len(df_eu_day.day.unique()))))
df_eu_day["day_num"] = df_eu_day.day.apply(lambda d: date_num[d])

columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
db_slot = pd.DataFrame(columns=columns)

print(df_eu_day.airline.unique().shape[0], "airlines")


i = 0
for airline in df_eu_day.airline.unique():
    print(i, airline)
    df_airline = df_eu_day[df_eu_day.airline == airline]
    db_s = check_airline_series(airline, df_airline, airport_list, week_day)
    db_slot = pd.concat([db_slot, db_s], ignore_index=True)
    i += 1


db_slot.to_csv("ok/slot_2018.csv", sep="\t", index=False, index_label=False)
# db_slot.to_csv("SeriesAnalysis/data_eu/db_slot_test.csv", index_label=False, index=False)

gf = {}
for airline in db_slot.Airline.unique():
    gf[airline] = Gf(airline)
    gf_air = db_slot[db_slot.Airline == airline]
    for i in range(gf_air.shape[0]):
        line = gf_air.iloc[i]
        gf[airline].add_gf(line.A_ICAO, line.InitialDate, line.FinalDate, line.Time)

df_gf = pd.DataFrame(columns=["airport", "airline", "start", "end", "day_num"])
for airline in gf.keys():
    df_gf = pd.concat([df_gf, gf[airline].get_df()], ignore_index=True)
df_gf.slots = df_gf.slots.astype(int)
df_gf.to_csv("ok/gf_test.csv", sep="\t", index=False, index_label=False)



airline = "RYR"
gd = Gf(airline, df_airline=db_slot[db_slot.Airline == airline])
