import numpy as np
import pandas as pd
from multiprocessing import Pool
import multiprocessing
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)


def compute_series(tup):
    df_in, airp, num = tup
    df_to_append = pd.DataFrame(columns=df_in.columns)

    for day in range(7):
        for key in ["departure", "arrival"]:
            hhh = df_in[df_in[key] == airp]
            hhh = hhh[hhh["week day"] == day]
            for cs in hhh["callsign"].unique():
                same_call = hhh[hhh["callsign"] == cs]
                other_key = "arrival" if key == "departure" else "departure"
                for other_orig_dest in same_call[other_key].unique():
                    if same_call[same_call[other_key] == other_orig_dest].shape[0] > 4:
                        series = same_call[same_call[other_key] == other_orig_dest].copy()
                        series["series code"] = series[key].values[0] + "_" + series[other_key].values[0] \
                                                + "_" + cs + "_" + str(day)
                        df_to_append = pd.concat([df_to_append, series], ignore_index=True)

    print(num)
    return df_to_append


def find_series(df_summer, year, save=False):
    t = time.time()
    airports = pd.read_csv("data/airports.csv")

    df_summer = df_summer.drop_duplicates().copy()

    df_summer.departure = df_summer.departure.replace(np.NaN, "UNKNOWN")
    df_summer.arrival = df_summer.arrival.replace(np.NaN, "UNKNOWN")

    split_df = []
    ind = 0
    for airport in airports["airport"][:5]:
        split_df.append((df_summer[(df_summer["departure"] == airport) ^ (df_summer["arrival"] == airport)].copy(),
                         airport, ind))
        ind += 1

    split_df = tuple(split_df)
    num_procs = multiprocessing.cpu_count()

    pool = Pool(num_procs)
    result = pool.map(compute_series, split_df)
    final_df = pd.concat(result, ignore_index=True)
    pool.close()
    pool.join()
    final_df = final_df.drop_duplicates(subset=["departure", "arrival", "week day", "day", "callsign", "dep time"])
    final_df["series"] = np.zeros(final_df.shape[0])

    ind = 0
    for series_code in final_df["series code"].unique():
        final_df.loc[final_df["series code"] == series_code, "series"] = ind
        ind += 1

    final_df["series"] = final_df["series"].astype(int)
    final_df["airline"] = final_df["callsign"].apply(lambda call: call[:3])

    if save:
        final_df.to_csv("data/series_"+str(year)+".csv", index=False)
    else:
        print(final_df)
    print(time.time()-t)


def compute_series_1(tup):
    df_in, key, cs, day = tup
    df_to_append = pd.DataFrame(columns=df_in.columns)
    other_key = "arrival" if key == "departure" else "departure"
    for other_orig_dest in df_in[other_key].unique():
        if df_in[df_in[other_key] == other_orig_dest].shape[0] > 4:
            series = df_in[df_in[other_key] == other_orig_dest].copy()
            series["series code"] = series[key].values[0] + "_" + series[other_key].values[0] \
                                    + "_" + cs + "_" + str(day)
            df_to_append = pd.concat([df_to_append, series], ignore_index=True)

    return df_to_append


def find_series_1(df_summer, year, save=False):
    t = time.time()
    airports = pd.read_csv("data/airports.csv")

    df_summer = df_summer.drop_duplicates().copy()

    df_summer.departure = df_summer.departure.replace(np.NaN, "UNKNOWN")
    df_summer.arrival = df_summer.arrival.replace(np.NaN, "UNKNOWN")

    final_df = pd.DataFrame(columns=df_summer.columns)
    num_procs = multiprocessing.cpu_count()

    ind = 0
    for airport in airports["airport"]:
        df_airport = df_summer[(df_summer["departure"] == airport) ^ (df_summer["arrival"] == airport)]
        for day in range(7):
            for key in ["departure", "arrival"]:
                pool = Pool(num_procs)
                hhh = df_airport[df_airport[key] == airport]
                hhh = hhh[hhh["week day"] == day]
                split_df = []
                for cs in hhh["callsign"].unique():
                    split_df.append([hhh[hhh["callsign"] == cs].copy(), key, cs, day])
                result = pool.map(compute_series_1, tuple(split_df))
                pool.close()
                pool.join()
                if len(result) > 0:
                    result = pd.concat(result, ignore_index=True)
                    final_df = pd.concat([final_df, result], ignore_index=True)
        print(airport, ind)
        ind += 1

    final_df = final_df.drop_duplicates(subset=["departure", "arrival", "week day", "day", "callsign", "dep time", "dep dist"])
    final_df["series"] = np.zeros(final_df.shape[0])
    ind = 0
    for series_code in final_df["series code"].unique():
        final_df.loc[final_df["series code"] == series_code, "series"] = ind
        ind += 1

    final_df["series"] = final_df["series"].astype(int)
    final_df["airline"] = final_df["callsign"].apply(lambda call: call[:3])
    print(time.time() - t)

    if save:
        final_df.to_csv("data/series_"+str(year)+".csv", index=False)
    else:
        print(final_df)
    print(time.time()-t)

print("yep")
df = pd.read_csv("data/summer_2018.csv")

find_series_1(df, 2018, save=True)
