import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Analysis import load_files as lf

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}

df_airports = pd.read_csv("DataAirport/airports.csv")
airport_list = df_airports[df_airports.level == 3].airport.to_list()
df_eu_airport = pd.read_csv("DataAirport/eu_airport.csv")
lev_12_airports = df_airports[(df_airports.level == 1) | (df_airports.level == 2)].airport.to_list()
all_airports = df_eu_airport.airport.to_list()

voli_18, voli_19 = lf.load_voli()
slot_18, slot_19 = lf.load_slot()
gf, new = lf.load_gf_new_entrant()

plt.rcParams["figure.figsize"] = (20, 15)
plt.rcParams.update({'font.size': 16})


def total_analysis(voli, slot, year):
    value = "-1" if year == 2018 else -1
    match = voli[voli.match != value]
    voli = voli[voli.match == value]


def make_fl_analysis(voli, slot, year):
    value = "-1" if year == 2018 else -1
    match = voli[voli.match != value]
    voli = voli[voli.match == value]
    airports = ["Total"] + airport_list

    fl = [voli.shape[0]] + [voli[voli.airport == airport].shape[0] for airport in airport_list]
    sl = [slot.shape[0]] + [slot[slot.A_ICAO == airport].shape[0] for airport in airport_list]

    matched = [match.shape[0]] + [match[match.airport == airport].shape[0] for airport in airport_list]
    df_voli = pd.DataFrame({"Airport": airports, "Flights": fl, "Matched": matched})
    df_slot = pd.DataFrame({"Airport": airports, "Series": sl})

    for day in day_dict.keys():
        voli_day = voli[voli["week day"] == day]
        fl = [voli_day.shape[0]] + [voli_day[voli_day.airport == airport].shape[0] for airport in airport_list]
        sl = [slot.shape[0]] + [slot[slot.A_ICAO == airport].shape[0] for airport in airport_list]
        df_voli[day_dict[day]] = fl
        df_slot[day_dict[day]] = sl

    return df_voli.sort_values(by="Flights", ascending=False), df_slot.sort_values(by="Series", ascending=False)


def plot_series_len(slot, year, save=False):
    len_series = slot[["id", "InitialDate", "FinalDate"]].copy()
    len_series["len"] = len_series.FinalDate - len_series.InitialDate + 1
    lens = sorted(len_series.len.unique())
    len_num_dict = {}
    for l in lens:
        len_num_dict[l] = len_series[len_series.len == l].shape[0]


    plt.grid(axis="y", zorder=0)
    plt.bar(len_num_dict.keys(), len_num_dict.values(), zorder=3)
    plt.title(str(year) + " SERIES LENGTH")
    plt.ylabel("Number of flights")
    plt.xlabel("Length")

    plt.xticks(range(5, 31), range(5, 31))
    if save:
        plt.savefig("plots/series_len_" + str(year), bbox_inches='tight')
    else:
        plt.show()


def plot_series_per_day(voli, year, save=False):
    for day in day_dict.keys():
        days = voli.day.unique()
        value = "-1" if year == 2018 else -1
        voli = voli[voli.match == value]
        per_day = [voli[(voli["week day"] == day) & (voli.airport.isin(airport_list) & (voli.day == d))].shape[0]
                   for d in days]
        plt.title(str(year) + " " + day_dict[day] + " FLIGHTS PER DAY")
        plt.ylabel("Number of flights")
        plt.xlabel("Day")
        plt.grid(axis="y", zorder=0)
        plt.bar(days, per_day, zorder=3)
        plt.xticks(days, days+1)
        if save:
            plt.savefig("plots/series_pre_day_" + str(year) + day_dict[day], bbox_inches='tight')
        else:
            plt.show()
        plt.close()


fl_18, sl_18 = make_fl_analysis(voli_18, slot_18, 2018)
print(fl_18.to_latex(index=False, caption="FLIGHTS 2018"))

print(sl_18.to_latex(index=False, caption="SERIES 2018"))

plot_series_len(slot_18, 2018, False)


fl_19, sl_19 = make_fl_analysis(voli_19, slot_19, 2019)
print(fl_19.to_latex(index=False, caption="FLIGHTS 2019"))

print(sl_19.to_latex(index=False, caption="SERIES 2019"))

plot_series_len(slot_19, 2019, False)

plot_series_per_day(voli_19, 2019, False)

voli_18.to_csv("DataGathered/voli_18.csv", index=False, index_label=False)
voli_19.to_csv("DataGathered/voli_19.csv", index=False, index_label=False)
slot_18.to_csv("DataGathered/slot_18.csv", index=False, index_label=False)
slot_19.to_csv("DataGathered/slot_19.csv", index=False, index_label=False)

summer = pd.read_csv("DataSummer/summer_2018.csv")
