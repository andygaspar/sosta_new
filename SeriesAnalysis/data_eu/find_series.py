from SeriesAnalysis.data_eu import eu_conversion as ec
import pandas as pd
import numpy as np

# aggiungere reference match
# aggiungere controllo gf
# aggiungere make df_db_slot

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

# airports
df_airports = pd.read_csv("SeriesAnalysis/data_eu/airports.csv")
airport_list = df_airports.airport.to_list()

# df_eu_clean
# df_eu = ec.read_and_set_df_eu(2019)
df_eu = pd.read_csv("SeriesAnalysis/data_eu/europe_cleaned.csv")


# 1946

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

            return True, approx_time(mean), init_day, final_day
        else:
            return False, None, None, None

    return False, None, None, None


def check_airline_series(df_airline):
    tol, max_occurrence, min_series_len = 30, 0.7, 5

    columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
    db_slot = pd.DataFrame(columns=columns)

    for callsign in df_airline.callsign.unique():
        df_call = df_airline[df_airline.callsign == callsign]

        # check multiple callsign per day
        if df_call.shape[0] >= min_series_len:
            if df_call.day.unique().shape[0] != df_call.shape[0]:
                df_call = df_call.drop_duplicates("day")

            departure, arrival = df_call.departure.iloc[0], df_call.arrival.iloc[0]

            is_departure, is_arrival = departure in airport_list, arrival in airport_list
            found_dep_series, found_arr_series = False, False

            mean_departure, mean_arrival, id_arrival = None, None, None

            # check series arrival
            if is_arrival:

                found_arr_series, mean_arrival, init_day, final_day \
                    = check_mean(df_call, False, tol, max_occurrence, min_series_len)
                if found_arr_series:
                    id_arrival = airline + departure + arrival + callsign + str(day)
                    to_append = [id_arrival] + [airline] + [arrival] + [mean_arrival] + [init_day] + [final_day] + ["N"]
                    db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

            # check series departure
            if is_departure:
                found_dep_series, mean_departure, init_day, final_day \
                    = check_mean(df_call, True, tol, max_occurrence, min_series_len)
                if found_dep_series:
                    id_departure = airline + departure + arrival + callsign + str(day)

                    if found_arr_series:
                        if mean_departure < mean_arrival:
                            to_append = \
                                [id_departure] + [airline] + [departure] + [mean_departure] \
                                + [init_day] + [final_day] + [id_arrival]
                            db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

                        else:
                            to_append = \
                                [id_departure] + [airline] + [departure] + [mean_departure] + [init_day] + \
                                [final_day] + ["N"]
                            db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

                    else:
                        to_append = \
                            [id_departure] + [airline] + [departure] + [mean_departure] \
                            + [init_day] + [final_day] + ["N"]
                        db_slot = db_slot.append(dict(zip(columns, to_append)), ignore_index=True)

    return db_slot

day = 1

df_eu_day = df_eu[df_eu["week day"] == day].copy()
date_num = dict(zip(np.sort(df_eu_day.day.unique()), range(len(df_eu_day.day.unique()))))
df_eu_day["day_num"] = df_eu_day.day.apply(lambda d: date_num[d])

airline = "RYR"
df_airline = df_eu_day[df_eu_day.airline == airline]

columns = ["id", "Airline", "A_ICAO", "Time", "InitialDate", "FinalDate", "matched"]
db_slot = pd.DataFrame(columns=columns)

i = 0
for airline in df_eu.airline.unique():
    print(i, airline)
    df_airline = df_eu_day[df_eu_day.airline == airline]
    db_slot = pd.concat([db_slot, check_airline_series(df_airline)], ignore_index=True)
    i += 1





