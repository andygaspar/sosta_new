from SeriesAnalysis.data_eu import eu_conversion as ec
import pandas as pd
import numpy as np


#controllo date inizio-fine stagione
#aggiungere giorno serie
#aggiungere referece match
#aggiungere controllo gf
#aggiungere make df_db_slot

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

#airports
df_airports = pd.read_csv("SeriesAnalysis/data_eu/airports.csv")
airport_list = df_airports.airport.to_list()

#df_eu_clean
df_eu = ec.read_and_set_df_eu()
df_eu_no_duplicates = df_eu.drop_duplicates()

df_ryr = df_eu_no_duplicates[df_eu_no_duplicates.airline == "RYR"]




df_ryr_1 = df_ryr[df_ryr["week day"] == 1]

def check_mean(df_call: pd.DataFrame, is_departure: bool, tol: int, max_occurrence: float, min_series_len: int):

    dep_arr = "dep_min" if is_departure else "arr_min"
    airport_dep_arr = "departure" if is_departure else "arrival"

    airport = df_call[airport_dep_arr].mode().values[0]
    df_call_airport = df_call[df_call[airport_dep_arr] == airport]

    if df_call_airport.shape[0] >= min_series_len:
        mean = df_call_airport[dep_arr].mean()
        if df_call_airport[(df_call_airport[dep_arr] <= mean + tol)
                           & (df_call_airport[dep_arr] >= mean - tol)].shape[0]\
                >= df_call_airport.shape[0] * max_occurrence:
            df_call_airp_filtered = df_call_airport[(df_call_airport[dep_arr] <= mean + 400)
                           & (df_call_airport[dep_arr] >= mean - 400)]
            mean = df_call_airp_filtered[dep_arr].mean()
            return True, mean
        else:
            return False, 0

    return False, 0


tol = 30
max_occurrence = 0.7
min_series_len = 5

dep = 0
arr = 0
match = 0
double = 0
issue = 0

for callsign in df_ryr_1.callsign.unique():
    df_call = df_ryr_1[df_ryr_1.callsign == callsign]

    # check multiple callsign per day
    if df_call.shape[0] >= min_series_len:
        if df_call.day.unique().shape[0] != df_call.shape[0]:

            df_call = df_call.drop_duplicates("day")
            # print("ok")

            # check airport
        is_departure = df_call.departure.iloc[0] in airport_list
        is_arrival = df_call.arrival.iloc[0] in airport_list
        found_arr_series = False
        found_dep_series = False

        mean_departure = None
        mean_arrival = None


        # check series arrival
        if is_arrival:

            found_arr_series, mean_arrival = check_mean(df_call, False, tol, max_occurrence, min_series_len)
            if found_arr_series:
                arr += 1


        # check series departure
        if is_departure:
            found_dep_series, mean_departure = check_mean(df_call, True, tol, max_occurrence, min_series_len)
            if found_dep_series:
                # print("seires dep", df_call.departure.iloc[0], callsign)
                dep += 1

        if is_departure and is_arrival:
            if found_arr_series and found_dep_series:
                # print("match ", mean_departure, mean_arrival)
                if mean_departure < mean_arrival:
                    match += 1

                else:
                    issue += 1
                    # print(mean_departure, mean_arrival)
                    # print(df_call)

        # else:
        #     print("issue double")
        #     double += 1
        #     print(df_call)
