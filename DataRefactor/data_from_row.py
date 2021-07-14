import pandas as pd
import numpy as np
import datetime

pd.set_option('display.max_columns', None)


def rename(df_in):
    renamed = df_in[["icao24", "firstseen", "estdepartureairport", "lastseen", "estarrivalairport", "callsign",
                     "estdepartureairporthorizdistance", "estdepartureairportvertdistance",
                     "estarrivalairporthorizdistance",
                     "estarrivalairportvertdistance", "departureairportcandidatescount",
                     "arrivalairportcandidatescount",
                     "day"]].copy()
    renamed.columns = ['icao24', "dep_time", 'departure', "arr_time", 'arrival', 'callsign', 'dep dist', 'dep alt',
                       'arr dist', 'arr alt', 'candidate dep airports', 'candidate arr airports', 'day']
    return renamed[['icao24', "dep_time", 'departure', "arr_time", 'arrival', 'callsign', 'day']]


def filter_airports(df_row):
    # df = pd.read_csv(df_row, sep="\t")
    df_airports = pd.read_csv("data/airports.csv", index_col=None).drop(columns="Unnamed: 0")
    df_eu = df_row[df_row.departure.isin(df_airports.airport)
                   | df_row.arrival.isin(df_airports.airport)].copy(deep=True)
    df_eu.departure = df_eu.departure.apply(lambda dep: "UNKNOWN" if type(dep) == float else dep)
    df_eu.arrival = df_eu.arrival.apply(lambda arr: "UNKNOWN" if type(arr) == float else arr)

    return df_eu


def day_converter(df_eu):
    df_eu.sort_values(by="day", inplace=True, ignore_index=True)
    df_eu["week day"] = df_eu["day"].apply(lambda d: datetime.datetime.fromtimestamp(d).weekday())
    df_eu["day"] = df_eu["day"].apply(lambda d: str(datetime.datetime.fromtimestamp(d))[:10])
    return df_eu


def time_minute_conversion(df_eu):
    time_ = df_eu["dep_time"].apply(lambda d: datetime.datetime.fromtimestamp(d).time() if not np.isnan(d) else "NaN")
    df_eu["dep_time"] = time_
    time_ = df_eu["arr_time"].apply(lambda d: datetime.datetime.fromtimestamp(d).time() if not np.isnan(d) else "NaN")
    df_eu["arr_time"] = time_
    t_min = df_eu["dep_time"].apply(
        lambda t: np.round(t.hour * 60 + t.minute + t.second * 0.1) if type(t) == datetime.time else 0).astype(int)
    df_eu["dep_min"] = t_min
    t_min = df_eu["arr_time"].apply(
        lambda t: np.round(t.hour * 60 + t.minute + t.second * 0.1) if type(t) == datetime.time else 0).astype(int)
    df_eu["arr_min"] = t_min
    return df_eu


def from_row_to_season(df_row, year, save=False):
    df_row = rename(df_row)
    final_df = filter_airports(df_row)
    final_df = day_converter(final_df)
    start = datetime.datetime(year, 3, 31) if year == 2019 else datetime.datetime(year, 4, 1)
    end = datetime.datetime(year, 10, 27) if year == 2019 else datetime.datetime(year, 10, 28)
    print(start, end, year)
    final_df = final_df[(pd.to_datetime(final_df["day"]) >= start) &
                        (pd.to_datetime(final_df["day"]) < end)]
    final_df = time_minute_conversion(final_df)
    final_df["airline"] = final_df["callsign"].apply(lambda call: call[:3])

    if save:
        final_df.to_csv("summer_"+str(year)+".csv", index_label=False, index=False)
    else:
        #print(final_df)
        return final_df

# from_row_to_season("data/data_row_fulvio_2019.csv", 2019, True)




