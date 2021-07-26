import pandas as pd
import numpy as np


def approx_time(t):
    time_approximation = int(t / 10) * 10 if (t // 5) % 2 == 0 else int(t / 10) * 10 + 5
    return time_approximation


def check_mean(df_call: pd.DataFrame, airport, is_departure: bool, tol: int, max_occurrence: float,
               min_series_len: int):
    dep_arr = "dep_min" if is_departure else "arr_min"
    airport_dep_arr = "departure" if is_departure else "arrival"

    df_call_airport = df_call[df_call[airport_dep_arr] == airport]

    if df_call_airport.shape[0] >= min_series_len:
        mean = df_call_airport[dep_arr].mean()
        df_30 = df_call_airport[(df_call_airport[dep_arr] <= mean + tol) & (df_call_airport[dep_arr] >= mean - tol)]
        if df_30.shape[0] >= df_call_airport.shape[0] * max_occurrence:
            df_call_airp_filtered = df_call_airport[(df_call_airport[dep_arr] <= mean + 180)
                                                    & (df_call_airport[dep_arr] >= mean - 60)]
            mean = df_30[dep_arr].mean()

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
                        = check_mean(df_call, departure, True, tol, max_occurrence, min_series_len)

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
                            to_append = [id_arrival, airline, arrival, mean_arrival, init_day, final_day, matched]
                            db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

                        else:
                            turn = 30 if arrival in all_airports else 90
                            if arrival in lev12_airports:
                                mean_arrival = approx_time(voli.arr_min.mean())
                                # here the series is refered to the departure one
                                id_arrs = [callsign + arrival + day for day in voli.day]
                                db_voli = make_df_voli(db_voli, False, voli, arrival, id_departure, mean_arrival,
                                                       id_arrs, None, turn, df_18)
                                if mean_departure < mean_arrival:
                                    id_arrs = None
                            else:
                                id_arrs = None
                            db_voli = make_df_voli(db_voli, True, voli, departure, id_departure, mean_departure,
                                                   id_deps, id_arrs, turn, df_18)

                # check series arrival
                elif is_arrival:

                    found_arr_series, mean_arrival, init_day, final_day, voli \
                        = check_mean(df_call, arrival, False, tol, max_occurrence, min_series_len)

                    if found_arr_series:
                        turn = 30 if departure in all_airports else 90
                        id_arrival = airline + arrival + departure + callsign + str(week_day)
                        id_arrs = [callsign + arrival + day for day in voli.day]
                        matched = "N"
                        # check airport lev12 for departure
                        if departure in lev12_airports:
                            # here the series is refered to the arrival one
                            mean_departure = approx_time(voli.dep_min.mean())
                            id_deps = [callsign + departure + day for day in voli.day]
                            if mean_departure < mean_arrival:
                                db_voli = make_df_voli(db_voli, True, voli, departure, id_arrival, mean_departure,
                                                       id_deps, id_arrs, turn, df_18)
                            else:
                                db_voli = make_df_voli(db_voli, True, voli, departure, id_arrival, mean_departure,
                                                       id_deps, None, turn, df_18)
                        to_append = [id_arrival, airline, arrival, mean_arrival, init_day, final_day, matched]
                        db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)
                        db_voli = make_df_voli(db_voli, False, voli, arrival, id_arrival,
                                               mean_arrival, id_arrs, None, turn, df_18)

    return db_slot, db_voli
