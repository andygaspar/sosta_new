import pandas as pd
import numpy as np
import datetime


def day_converter(df_in):
    df_in.sort_values(by="day", inplace=True, ignore_index=True)
    df_in["week day"] = df_in["day"].apply(lambda d: datetime.datetime.fromtimestamp(d).weekday())
    df_in["day"] = df_in["day"].apply(lambda d: str(datetime.datetime.fromtimestamp(d))[:10])
    return df_in


def read_and_set_df_eu(year: int):
    df_eu = pd.read_csv("SeriesAnalysis/data_eu/europe_" + str(year) + ".csv")
    df_eu = df_eu.drop_duplicates()

    df_eu = df_eu[['icao24', 'dep time', 'departure', 'arr time', 'arrival', 'callsign', 'day', 'week day']].copy()
    print(df_eu.shape, "sahpe")

    # from timestamp to datetime
    time_ = df_eu["dep time"].apply(lambda d: datetime.datetime.fromtimestamp(d).time() if not np.isnan(d) else "NaN")
    df_eu["dep time"] = time_
    time_ = df_eu["arr time"].apply(lambda d: datetime.datetime.fromtimestamp(d).time() if not np.isnan(d) else "NaN")
    df_eu["arr time"] = time_
    # removing NaN
    # df_eu = df_eu[(df_eu["dep time"] != "NaN") & (df_eu["arr time"] != "NaN")]
    # df_eu = df_eu[~pd.isna(df_eu.arrival) & ~pd.isna(df_eu.departure)]


    # from datetime to minute
    t_min = df_eu["dep time"].apply(
        lambda t: np.round(t.hour * 60 + t.minute + t.second * 0.1) if t != "NaN" else 0).astype(int)
    df_eu["dep_min"] = t_min
    t_min = df_eu["arr time"].apply(
        lambda t: np.round(t.hour * 60 + t.minute + t.second * 0.1) if t != "NaN" else 0).astype(int)
    df_eu["arr_min"] = t_min

    # airline
    df_eu["airline"] = df_eu["callsign"].apply(lambda call: call[:3])
    start = datetime.datetime(year, 3, 31) if year == 2019 else datetime.datetime(year, 4, 1)
    end = datetime.datetime(year, 10, 27) if year == 2019 else datetime.datetime(year, 10, 28)
    df_eu = df_eu[(pd.to_datetime(df_eu["day"]) >= start) &
                  (pd.to_datetime(df_eu["day"]) < end)]

    return df_eu



"""
time assignment

final_df["time_request"] = final_df.mean_time.apply(
            lambda t: int(t / 10) * 10 if (t // 5) % 2 == 0 else int(t / 10) * 10 + 5).astype(int)
        return final_df
"""

