import pandas as pd
import numpy as np
from DataRefactor import data_from_row as dfr
import datetime

# df_row = pd.read_csv("data/data_row_fulvio_2019.csv", sep="\t")
# df_row = dfr.rename(df_row)
# df_airports = pd.read_csv("data/airports.csv")
# df_eu = df_row[df_row.departure.isin(df_airports.airport) ^ df_row.arrival.isin(df_airports.airport)].copy(deep=True)
# df_eu = dfr.day_converter(df_eu)
# time_ = df_eu["dep_time"].apply(lambda d: datetime.datetime.fromtimestamp(d).time() if not np.isnan(d) else "NaN")
# df_eu["dep_time"] = time_
#
#
# type(df_eu["dep_time"][0]) == datetime.time
#
# t_min = df_eu["dep_time"].apply(
#     lambda t: np.round(t.hour * 60 + t.minute + t.second * 0.1) if type(t) == datetime.time else 0).astype(int)

df_row = pd.read_csv("data/data_row_fulvio_2019.csv", sep="\t")
dfr.from_row_to_season(df_row, 2019, True)

df_row = pd.read_csv("data/data_row_fulvio_2018.csv", sep="\t")
dfr.from_row_to_season(df_row, 2018, True)

df_19 = pd.read_csv("summer_2019.csv")
df_18 = pd.read_csv("summer_2018.csv")
