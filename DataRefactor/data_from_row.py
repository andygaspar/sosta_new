import pandas as pd
import datetime

pd.set_option('display.max_columns', None)


def filter_airports(df_fulvio):
    df = pd.read_csv(df_fulvio, sep="\t")

    airports = pd.read_csv("data/airports.csv", index_col=None).drop(columns="Unnamed: 0")
    final_df = pd.DataFrame(columns=df.columns)
    i = 0
    for airport in airports["airport"]:
        print(airport, i)
        i += 1
        temp = df[(df["estdepartureairport"] == airport) ^ (df["estarrivalairport"] == airport)]
        final_df = pd.concat([final_df, temp])

    return final_df


def rename(df_in):
    renamed = df_in[["icao24", "firstseen", "estdepartureairport", "lastseen", "estarrivalairport", "callsign",
                     "estdepartureairporthorizdistance", "estdepartureairportvertdistance",
                     "estarrivalairporthorizdistance",
                     "estarrivalairportvertdistance", "departureairportcandidatescount",
                     "arrivalairportcandidatescount",
                     "day"]].copy()
    renamed.columns = ['icao24', "dep time", 'departure', "arr time", 'arrival', 'callsign', 'dep dist', 'dep alt',
                       'arr dist', 'arr alt', 'candidate dep airports', 'candidate arr airports', 'day']
    return renamed


def day_converter(df_in):
    df_in.sort_values(by="day", inplace=True, ignore_index=True)
    df_in["week day"] = df_in["day"].apply(lambda d: datetime.datetime.fromtimestamp(d).weekday())
    df_in["day"] = df_in["day"].apply(lambda d: str(datetime.datetime.fromtimestamp(d))[:10])
    return df_in


def from_row_to_season(df_row, year, save=False):
    final_df = filter_airports(df_row)

    # final_df.to_csv("europe"+str(year)+".csv")
    final_df = rename(final_df)
    final_df = day_converter(final_df)
    start = datetime.datetime(year, 3, 31) if year == 2019 else datetime.datetime(year, 4, 1)
    end = datetime.datetime(year, 10, 27) if year == 2019 else datetime.datetime(year, 10, 28)
    final_df = final_df[(pd.to_datetime(final_df["day"]) >= start) &
                        (pd.to_datetime(final_df["day"]) < end)]

    if save:
        final_df.to_csv("data/summer_"+str(year)+".csv", index_label=False, index=False)
    else:
        print(final_df)

from_row_to_season("data/data_row_fulvio_2019.csv", 2019, True)




