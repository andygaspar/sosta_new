import pandas as pd
import numpy as np



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


def check_gf(voli, voli_18, voli_18_air, time):
    is_gf = []
    for d in voli.day_num:
        v_18_day = voli_18_air[(voli_18_air.day == d) & (voli_18_air.gf == False)]
        if v_18_day.shape[0] > 0:
            gap, index = abs(v_18_day.time - time).min(), abs(v_18_day.time - time).idxmin()
            if gap <= 30:
                voli_18.at[index, "gf"] = True
                is_gf.append("Y")
            else:
                is_gf.append("N")
        else:
            is_gf.append("N")

    return is_gf


def make_df_voli(db_voli, is_departure: bool, voli: pd.DataFrame, airport, series,
                 time, id_fls, match, turn, voli_18):

    if voli_18 is not None:
        dep_arr, flow_ = ("D", "D") if is_departure else ("A", "A")
        v_18_air = voli_18[(voli_18.airport == airport) & (voli_18.flow == "D")]
        gf = check_gf(voli, voli_18, v_18_air, time)
    else:
        flow_ = "D" if is_departure else "A"
        gf = ["N" for _ in range(voli.shape[0])]
    flow = [flow_ for _ in range(voli.shape[0])]
    airp = [airport for _ in range(voli.shape[0])]
    times = [time for _ in range(voli.shape[0])]
    ser = [series for _ in range(voli.shape[0])]
    turns = [turn for _ in range(voli.shape[0])]
    csvt = ["J" for _ in range(voli.shape[0])]
    if match is None:
        match = [-1 for _ in range(voli.shape[0])]
    to_concat = pd.DataFrame({"id": id_fls, "airline": voli.airline.copy(deep=True), "flow": flow,
                              "airport": airp, "icao24": voli.icao24.copy(deep=True),
                              "day": voli.day_num,
                              "time": times, "series": ser, "CSVT": csvt, "gf": gf,
                              "turnaround": turns, "match": match, "callsign": voli.callsign})
    return pd.concat([db_voli, to_concat], ignore_index=True)


def check_airline_series(airline, df_airline, airport_list_3, lev12_airports, all_airports, week_day, df_18=None):
    tol, max_occurrence, min_series_len = 30, 0.7, 5

    columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
    db_slot = pd.DataFrame(columns=columns)

    cols_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
                 "CSVT", "gf", "turnaround", "match", "callsign"]
    db_voli = pd.DataFrame(columns=cols_voli)

    for callsign in df_airline.callsign.unique():
        df_call = df_airline[df_airline.callsign == callsign]

        # check multiple callsign per day
        if df_call.shape[0] >= min_series_len:
            if df_call.day.unique().shape[0] != df_call.shape[0]:
                df_call = df_call.drop_duplicates("day")

            departure, arrival = df_call.departure.mode().values[0], df_call.arrival.mode().values[0]

            if departure != arrival:

                is_departure, is_arrival = departure in airport_list_3, arrival in airport_list_3

                if is_departure:
                    found_dep_series, mean_departure, init_day, final_day, voli \
                        = check_mean(df_call, True, tol, max_occurrence, min_series_len)

                    if found_dep_series:
                        id_departure = airline + departure + arrival + callsign + str(week_day)
                        to_append = [id_departure, airline, departure, mean_departure, init_day, final_day, "N"]
                        db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)
                        id_deps = [callsign + departure + day for day in voli.day]

                        if is_arrival:
                            turn = 30 if arrival in all_airports else 90
                            mean_arrival = approx_time(voli.arr_min.mean())
                            id_arrival = airline + arrival + departure + callsign + str(week_day)
                            id_arrs = [callsign + arrival + day for day in voli.day]
                            matched = "N"
                            if mean_departure < mean_arrival:
                                matched = id_departure
                                db_voli = make_df_voli(db_voli, True, voli, departure, id_departure, mean_departure,
                                                       id_deps, id_arrs, turn, df_18)
                                db_voli = make_df_voli(db_voli, False, voli, arrival, id_arrival, mean_arrival,
                                                       id_arrs, None, turn, df_18)
                            else:
                                db_voli = make_df_voli(db_voli, True, voli, departure, id_departure, mean_departure,
                                                       id_deps, None, turn, df_18)
                                db_voli = make_df_voli(db_voli, False, voli, arrival, id_arrival, mean_arrival,
                                                       id_arrs, None, turn, df_18)
                            to_append = \
                                [id_arrival] + [airline] + [arrival] + [mean_arrival] \
                                + [init_day] + [final_day] + [matched]
                            db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

                        else:
                            turn = 30 if arrival in all_airports else 90
                            if arrival in lev12_airports:
                                mean_arrival = approx_time(voli.arr_min.mean())
                                id_arrival = airline + arrival + departure + callsign + str(week_day)
                                id_arrs = [callsign + arrival + day for day in voli.day]
                                db_voli = make_df_voli(db_voli, False, voli, arrival, id_arrival, mean_arrival,
                                                       id_arrs, None, turn, df_18)
                            else:
                                id_arrs = None
                            db_voli = make_df_voli(db_voli, True, voli, departure, id_departure, mean_departure,
                                                   id_deps, id_arrs, turn, df_18)



                # check series arrival
                if is_arrival and not is_departure:

                    found_arr_series, mean_arrival, init_day, final_day, voli \
                        = check_mean(df_call, False, tol, max_occurrence, min_series_len)

                    if found_arr_series:
                        turn = 30 if arrival in all_airports else 90
                        id_arrival = airline + arrival + departure + callsign + str(week_day)
                        id_arrs = [callsign + arrival + day for day in voli.day]
                        matched = "N"
                        if departure in lev12_airports:
                            id_departure = airline + departure + arrival + callsign + str(week_day)
                            matched = id_departure
                            mean_departure = approx_time(voli.dep_min.mean())
                            id_deps = [callsign + departure + day for day in voli.day]
                            db_voli = make_df_voli(db_voli, True, voli, departure, id_departure, mean_departure,
                                                   id_deps, id_arrs, turn, df_18)
                        to_append = [id_arrival, airline, arrival, mean_arrival, init_day, final_day, matched]
                        db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)
                        db_voli = make_df_voli(db_voli, False, voli, arrival, id_arrival,
                                               mean_arrival, id_arrs, None, turn, df_18)


    return db_slot, db_voli

#21961

# 19437
# 3634
# week_day = 1
#
# df_eu_day = df_eu[df_eu["week day"] == week_day].copy()
# date_num = dict(zip(np.sort(df_eu_day.day.unique()), range(len(df_eu_day.day.unique()))))
# df_eu_day["day_num"] = df_eu_day.day.apply(lambda d: date_num[d])
#
# columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
# db_slot = pd.DataFrame(columns=columns)
#
# print(df_eu_day.airline.unique().shape[0], "airlines")
#
# cols_db_voli = ["id", "airline", "flow", "airport", "icao24", "day", "time", "series",
#                  "CSVT", "gf", "turnaround", "match", "callsign"]
# db_voli = pd.DataFrame(columns=cols_db_voli)
#
# voli_2018 = pd.read_csv("ok/voli_2018.csv", sep="\t")
# voli_2018["gf"] = [False for _ in range(voli_2018.shape[0])]
#
# i = 0
# for airline in df_eu_day.airline.unique():
#     print(i, airline)
#     df_airline, df_air_18 = df_eu_day[df_eu_day.airline == airline], voli_2018[voli_2018.airline == airline]
#     db_s, db_v = check_airline_series(airline, df_airline, df_air_18, airport_list_3, week_day)
#     db_slot = pd.concat([db_slot, db_s], ignore_index=True)
#     db_voli = pd.concat([db_voli, db_v], ignore_index=True)
#     i += 1
#
# db_slot.to_csv("ok/slot_2019.csv", sep="\t", index=False, index_label=False)
# db_voli.to_csv("ok/voli_2019.csv", sep="\t", index=False, index_label=False)

# db_slot.to_csv("SeriesAnalysis/data_eu/db_slot_test.csv", index_label=False, index=False)


