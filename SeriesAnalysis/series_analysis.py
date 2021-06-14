import copy
import multiprocessing

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import multiprocess
from multiprocess import Pool
from SeriesAnalysis import funs as f

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

num_procs = multiprocess.cpu_count()
print(num_procs)

start_minute = np.array([0, 540, 900, 1140])
end_minute = np.array([539, 899, 1139, 1440])


def get_interval(time):
    for i in range(len(start_minute)):
        if start_minute[i] <= time <= end_minute[i]:
            return i
    return 3


def is_gran_fathered(depa, arri, air, time, is_dep, df_18, gf_as):
    dep_arr = "departure" if is_dep else "arrival"
    dep_arr_min = "dep time minute" if is_dep else "arr time minute"
    airp = depa if is_dep else arri
    g = gf_as[(gf_as["airport"] == airp) & (gf_as["airline"] == air) &
              (gf_as["start"] == start_minute[get_interval(time)])]
    if g.shape[0] == 0:
        return False
    condition = g["assigned"].values[0] < g["slots"].values[0]
    if condition:
        if df_18[(df_18["departure"] == depa) & (df_18["arrival"] == arri) &
                 ((df_18[dep_arr_min] <= time + 30) ^ (df_18[dep_arr_min] >= time - 30)) &
                 (df_18["airline"] == air)].shape[0] > 0:
            return True
        elif df_18[(df_18[dep_arr] == airp) & ((df_18[dep_arr_min] <= time + 30) ^ (df_18[dep_arr_min] >= time - 30)) &
                   (df_18["airline"] == air)].shape[0] > 0:
            return True
    else:
        return False


def turn_around(depa, arri):
    if depa[:2] in europe_codes and arri[:2] in europe_codes:
        return 30
    else:
        return 90


df = pd.read_csv('data/series_2019.csv')
airports = pd.read_csv('data/airports.csv')
europe_codes = np.unique([air[:2] for air in airports.airport])

departures_2019, arrivals_2019 = f.get_departure_arrivals(df, airports)


is_departure = True
is_arrival = False

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", }
# tol = [30, 35, 40, 45, 50, 55, 60]
# min_oc = [0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
# f.find_series_plot(departures_2019, day, call_icao, num_procs, departure, 2019, save=True)

day = 5
tol = 30
max_occurence = 0.7
save = True

# for day in range(7):
series_2019_departures = f.find_series(departures_2019, day, tol, max_occurence, is_departure, num_procs)
series_2019_departures = f.add_midnights(series_2019_departures, departures_2019, day, is_departure)
series_2019_arrival = f.find_series(arrivals_2019, day, tol, max_occurence, is_arrival, num_procs)
series_2019_arrival = f.add_midnights(series_2019_arrival, arrivals_2019, day, is_arrival)

series_2019_departures[series_2019_departures.series == 215123]

# date_to_num = dict(zip(np.sort(series_2019_departures.day.unique()), range(len(series_2019_departures.day.unique()))))
# num_to_date = dict(zip(range(len(series_2019_departures.day.unique())), np.sort(series_2019_departures.day.unique())))

"""
filter by day and series
"""

departures_2019_day = departures_2019[(departures_2019["week day"] == day) &
                                      (departures_2019.series.isin(series_2019_departures.series))].copy()
arrivals_2019_day = arrivals_2019[(arrivals_2019["week day"] == day) &
                                  (arrivals_2019.series.isin(series_2019_arrival.series))].copy()


departures_2019_day.series.unique().shape
series_2019_departures.series.unique().shape
"""
fixing mismatching dep arr
"""

dep = series_2019_departures[series_2019_departures.arrival.isin(airports.airport)]
arr = series_2019_arrival[series_2019_arrival.departure.isin(airports.airport)]

not_ok_arr = dep[~dep.callsign.isin(arr.callsign.to_list())]
not_ok_dep = arr[~arr.callsign.isin(dep.callsign.to_list())]

d = departures_2019_day[departures_2019_day.callsign.isin(not_ok_arr.callsign.to_list())]
a = arrivals_2019_day[arrivals_2019_day.callsign.isin(not_ok_arr.callsign.to_list())]

columns = ['icao24', 'departure', 'arrival', 'callsign', 'day', 'week day', 'series code', 'series',
           'airline', 'is_departure', 'mean_time', 'start_day', 'len_series']

date_num = dict(zip(np.sort(departures_2019_day.day.unique()), range(len(departures_2019_day.day.unique()))))
departures_2019_day["day_num"] = departures_2019_day.day.apply(lambda dd: date_num[dd])
date_num_ = dict(zip(np.sort(arrivals_2019_day.day.unique()), range(len(arrivals_2019_day.day.unique()))))
arrivals_2019_day["day_num"] = arrivals_2019_day.day.apply(lambda dd: date_num_[dd])

series_2019_departures, departures_2019_day, series_2019_arrival, arrivals_2019_day, departure_series_index \
    = f.find_mismatches(not_ok_dep=not_ok_dep, series_arrival=series_2019_arrival, departures_day=departures_2019_day,
                        not_ok_arr=not_ok_arr, series_departure=series_2019_departures, arrival_day=arrivals_2019_day,
                        airports=airports, columns=columns)
departure_series_index = max(series_2019_departures.series)
# fixing index series arrival
series_2019_arrival.series = series_2019_arrival.series + departure_series_index + 1
arrivals_2019_day.series = arrivals_2019_day.series + departure_series_index + 1

# final day all series
series_2019_departures.time_request = series_2019_departures.time_request.astype(int)
series_2019_arrival.time_request = series_2019_arrival.time_request.astype(int)
departures_2019_day["dep time minute"] = departures_2019_day["dep time minute"].astype(int)
arrivals_2019_day["arr time minute"] = arrivals_2019_day["arr time minute"].astype(int)

# all_series = pd.concat([series_2019_departures, series_2019_arrival], ignore_index=True)

final_dep = departures_2019_day[
    ["airline", "departure", "icao24", "day", "dep time minute", "series", "callsign"]].copy()
date_to_num = dict(zip(np.sort(final_dep.day.unique()), range(len(final_dep.day.unique()))))
final_dep.day = final_dep.day.apply(lambda dd: date_to_num[dd])
final_dep["flow"] = ["D" for _ in range(final_dep.shape[0])]
final_dep["gf"] = ["N" for _ in range(final_dep.shape[0])]
final_dep["turnaround"] = [0 for _ in range(final_dep.shape[0])]
final_dep["CSVT"] = ["J" for _ in range(final_dep.shape[0])]
final_dep["match"] = [-1 for _ in range(final_dep.shape[0])]
final_dep["id"] = range(final_dep.shape[0])

final_arr = arrivals_2019_day[["airline","departure", "arrival", "icao24", "day", "arr time minute", "series", "callsign"]].copy()
date_to_num_ = dict(zip(np.sort(final_arr.day.unique()), range(len(final_arr.day.unique()))))
final_arr.day = final_arr.day.apply(lambda dd: date_to_num_[dd])
final_arr["flow"] = ["A" for _ in range(final_arr.shape[0])]
final_arr["gf"] = ["N" for _ in range(final_arr.shape[0])]
final_arr["turnaround"] = [0 for _ in range(final_arr.shape[0])]
final_arr["CSVT"] = ["J" for _ in range(final_arr.shape[0])]
final_arr["match"] = [-1 for _ in range(final_arr.shape[0])]
final_arr["id"] = range(final_dep.shape[0], final_dep.shape[0] + final_arr.shape[0])

"""
DBslot making
"""

# db_slot_d = pd.DataFrame({"id": series_2019_departures.series, "Airline": series_2019_departures.airline,
#                           "A_ICAO": series_2019_departures.departure, "Time": series_2019_departures.time_request,
#                           "InitialDate": series_2019_departures.start_day,
#                           "FinalDate": series_2019_departures.start_day + series_2019_departures.len_series -1})
#
# db_slot_a = pd.DataFrame({"id": series_2019_arrival.series, "Airline": series_2019_arrival.airline,
#                           "A_ICAO": series_2019_arrival.departure, "Time": series_2019_arrival.time_request,
#                           "InitialDate": series_2019_arrival.start_day,
#                           "FinalDate": series_2019_arrival.start_day + series_2019_arrival.len_series -1})
# db_slot = pd.concat([db_slot_a, db_slot_d], ignore_index=True)
# # if save:
# #     db_slot.to_csv("data/final/DBslot_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")
# db_slot.to_csv("DBslot_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")

"""
2018
"""

df = pd.read_csv('data/series_2018.csv')
airports = pd.read_csv('data/airports.csv')
departures_2018, arrivals_2018 = f.get_departure_arrivals(df, airports)

series_2018_departure = f.find_series(departures_2018, day, tol + 10, max_occurence, is_departure, num_procs)
series_2018_arrival = f.find_series(arrivals_2018, day, tol + 10, max_occurence, is_arrival, num_procs)
series_2018_departure = f.add_midnights(series_2018_departure, departures_2018, day, is_departure)
series_2018_arrival = f.add_midnights(series_2018_arrival, arrivals_2018, day, is_arrival)

departures_2018_day = departures_2018[(departures_2018["week day"] == day) &
                                      (departures_2018.series.isin(series_2018_departure.series))].copy()
arrivals_2018_day = arrivals_2018[(arrivals_2018["week day"] == day) &
                                  (arrivals_2018.series.isin(series_2018_arrival.series))].copy()

gf = f.get_gf_rights(departures_2018_day, arrivals_2018_day, num_procs)
date_to_num_gf = dict(zip(np.sort(departures_2018_day.day.unique()), range(len(departures_2018_day.day.unique()))))
gf["day_num"] = gf.day.apply(lambda dd: date_to_num_gf[dd])
gf = gf[["airport", "airline", "start", "end", "slots", "day_num"]]


# gf.to_csv("data/final/gf_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")

"""
GF per DB voli
"""

gf_assign = gf.copy()
gf_assign["assigned"] = np.zeros(gf_assign.shape[0]).astype(int)
gf.to_csv("test_friday/gf_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")
# df_test = final_dep.merge(final_arr, how="left", on=list(final_dep.columns), )

for i in range(final_dep.shape[0]):
    if i % 10000 == 0:
        print(i)

    fl = final_dep.iloc[i]
    dep = departures_2019_day[
        (departures_2019_day.callsign == fl.callsign) & (departures_2019_day.day_num == fl.day)].iloc[0]
    if is_gran_fathered(dep.departure, dep.arrival, dep.airline, dep["dep time minute"], True,
                        departures_2018_day, gf_assign):
        final_dep.iloc[i, final_dep.columns.get_loc("gf")] = "Y"
        gf_assign.loc[(gf_assign["airport"] == dep.departure) & (gf_assign["airline"] == dep.airline) &
                      (gf_assign["start"] == start_minute[get_interval(dep["dep time minute"])]), "assigned"] += 1
    final_dep.iloc[i, final_dep.columns.get_loc("turnaround")] = turn_around(dep.departure, dep.arrival)
    final = final_arr[(final_arr.callsign == dep.callsign) & (final_arr.day == dep.day_num) &
                      (final_arr.departure == dep.departure) & (final_arr.arrival == dep.arrival)]
    if final.shape[0] == 1:
        final_dep.iloc[i, final_dep.columns.get_loc("match")] = final.id.values[0]

for i in range(final_arr.shape[0]):
    fl = final_arr.iloc[i]
    arr = arrivals_2019_day[
        (arrivals_2019_day.callsign == fl.callsign) & (arrivals_2019_day.day_num == fl.day)].iloc[0]
    if is_gran_fathered(arr.departure, arr.arrival, arr.airline, arr["arr time minute"], False,
                        departures_2018_day, gf_assign):
        final_arr.iloc[i, final_arr.columns.get_loc("gf")] = "Y"
        gf_assign.loc[(gf_assign["airport"] == arr.arrival) & (gf_assign["airline"] == arr.airline) &
                      (gf_assign["start"] == start_minute[get_interval(arr["arr time minute"])]), "assigned"] += 1
    final_arr.iloc[i, final_arr.columns.get_loc("turnaround")] = turn_around(arr.departure, arr.arrival)



new_cols = list(final_dep.columns)
new_cols[1] = "airport"
new_cols[4] = "time"
final_dep.columns = new_cols
final_arr = final_arr.drop(columns=["departure"])

final_arr.columns = new_cols

final_dep.time = np.where(final_dep.time > 1439, 1439, final_dep.time)
final_arr.time = np.where(final_arr.time > 1439, 1439, final_arr.time)
final_dep.time = np.where(final_dep.time < 0, 0, final_dep.time)
final_arr.time = np.where(final_arr.time < 0, 0, final_arr.time)

db_voli = pd.concat([final_dep, final_arr], ignore_index=True)
db_voli = db_voli[["id", "airline", "flow", "airport", "icao24", "day", "time","series", "CSVT", "gf",
                   "turnaround", "match", "callsign"]]

db_voli["-4"] = np.ones(db_voli.shape[0]).astype(int) * (-4)
db_voli["-4p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["-3"] = np.ones(db_voli.shape[0]).astype(int) * (-3)
db_voli["-3p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["-2"] = np.ones(db_voli.shape[0]).astype(int) * (-2)
db_voli["-2p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["-1"] = np.ones(db_voli.shape[0]).astype(int) * (-1)
db_voli["-1p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["0"] = np.zeros(db_voli.shape[0]).astype(int)
db_voli["0p"] = np.ones(db_voli.shape[0]).astype(int)

db_voli["1"] = np.ones(db_voli.shape[0]).astype(int) * (1)
db_voli["1p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["2"] = np.ones(db_voli.shape[0]).astype(int) * (2)
db_voli["2p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["3"] = np.ones(db_voli.shape[0]).astype(int) * (3)
db_voli["3p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli["4"] = np.ones(db_voli.shape[0]).astype(int) * (4)
db_voli["4p"] = np.zeros(db_voli.shape[0]).astype(int)

db_voli = db_voli.drop_duplicates((["series", "day"]))

if save:
    db_voli.to_csv("data/final/DBvoli_" + day_dict[day] + ".txt", index_label=False, index=False, sep="\t")

# db_voli = pd.read_csv("data/final/DBvoli_Monday.txt", sep="\t")
# gf = pd.read_csv("data/final/gf_Monday.txt", sep="\t")

new_entrant = pd.DataFrame(columns=["airport", "airline", "day"])
for airport in airports.airport[3]:
    airp_db = db_voli[db_voli.airport == airport]
    for d in db_voli.day.unique():
        airp_day_db = airp_db[airp_db.day == d]
        for airline in airp_day_db.airline.unique():
            airp_day_airl_db = airp_day_db[airp_day_db.airline == airline]
            found = False
            i = 0
            while i < 4 and not found:
                g = gf[(gf.airport == airport) & (gf.day_num == d) &
                   (gf.airline == airline) & (gf.start == start_minute[i])]
                if g.shape[0] == 0:
                    new_entrant = new_entrant.append({"airport": airport, "airline": airline, "day":d}, ignore_index=True)
                    found = True
                else:
                    gf_slots = g.slots.values[0]
                    val = airp_day_airl_db[(start_minute[i] <= airp_day_airl_db.time) &
                                           (airp_day_airl_db.time < end_minute[i])].shape[0]
                    if val > gf_slots:
                        new_entrant = new_entrant.append({"airport": airport, "airline": airline, "day": d}, ignore_index=True)
                        found = True
                i += 1

if save:
    new_entrant.to_csv("data/final/new_entrant_" + day_dict[day] + ".txt", index_label=False, index=False, sep="\t")


" make db slot"

series_departure_filtered = series_2019_departures[series_2019_departures.series.isin(db_voli.series)]
series_arrival_filtered = series_2019_arrival[series_2019_arrival.series.isin(db_voli.series)]

print("departure series", series_2019_departures.series.shape[0], series_departure_filtered.series.shape[0])
print("arrival series", series_2019_arrival.series.shape[0], series_arrival_filtered.series.shape[0])

db_slot_d = pd.DataFrame({"id": series_departure_filtered.series, "Airline": series_departure_filtered.airline,
                          "A_ICAO": series_departure_filtered.departure, "Time": series_departure_filtered.time_request,
                          "InitialDate": series_departure_filtered.start_day,
                          "FinalDate": series_departure_filtered.start_day + series_departure_filtered.len_series -1})

db_slot_a = pd.DataFrame({"id": series_arrival_filtered.series, "Airline": series_arrival_filtered.airline,
                          "A_ICAO": series_arrival_filtered.departure, "Time": series_arrival_filtered.time_request,
                          "InitialDate": series_arrival_filtered.start_day,
                          "FinalDate": series_arrival_filtered.start_day + series_arrival_filtered.len_series -1})

db_slot = pd.concat([db_slot_a, db_slot_d], ignore_index=True)

#  TO FIX ARRIVAL TIME MIDNIGHTS *******************************************************

if save:
    db_slot.to_csv("data/final/DBslot_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")

db_slot.to_csv("test_friday/DBslot_" + day_dict[day] + ".txt", index_label=False, index=False, sep="\t")
db_voli.to_csv("test_friday/DBvoli_" + day_dict[day] + ".txt", index_label=False, index=False, sep="\t")

"""
db_slot.to_csv("DBslot_"+day_dict[day]+".txt", index_label=False, index=False, sep="\t")




# 215123
max(series_2019_departures.series)

db_voli

departures_2019[departures_2019.series == 215123]

db_slot[db_slot.id==215123]

db_voli[db_voli.id==277948]

db_voli.series.unique().shape
db_slot.id.unique().shape


for ser in db_voli.series.unique():
    if ser not in db_slot.id.to_list():
        print("ciao", ser)

for ser in db_slot.id.unique():
    if ser not in list(db_voli.series.unique()):
        print("non ciao", ser)

import copy

new_initial_day = copy.deepcopy(db_slot.InitialDate)
for i in range(db_slot.shape[0]):
    row = db_slot.iloc[i]
    ser = db_voli[db_voli.series == row.id]
    if min(ser.day) != row.InitialDate:
        print("problem")
        print(row.InitialDate, min(ser.day))
        print(ser)
        new_initial_day[i] = min(ser.day)

db_slot.InitialDate = new_initial_day


for i in range(db_slot.shape[0]):
    row = db_slot.iloc[i]
    ser = db_voli[db_voli.series == row.id]
    if min(ser.day) != row.InitialDate:
        print("problem")
        print(row.InitialDate, min(ser.day))
        print(ser)


for i in range(db_slot.shape[0]):
    row = db_slot.iloc[i]
    ser = db_voli[db_voli.series == row.id]
    if max(ser.day) != row.FinalDate:
        print("problem")
        print(row.FinalDate, max(ser.day))
        print(ser)

new_final_day = copy.deepcopy(db_slot.FinalDate)

for i in range(db_slot.shape[0]):
    row = db_slot.iloc[i]
    ser = db_voli[db_voli.series == row.id]
    if max(ser.day) != row.FinalDate:
        print("problem")
        print(row.FinalDate, max(ser.day))
        print(ser)
        new_final_day[i] = max(ser.day)

db_slot.FinalDate = new_final_day


db_voli[db_voli.id == 1516]
db_voli[db_voli.id == 278813]
print("   ")


db_voli[db_voli.series == 216265]

db_slot[db_slot.id == 1148]

time_request = np.zeros(db_slot.shape[0])
i = 0
for ser in db_slot.id:
    times = db_voli[db_voli.series == ser]["time"]
    time_request[i] = np.mean(times)
    i += 1
ggg = pd.DataFrame({"id_series":db_slot.id,"request":db_slot.Time, "mean": time_request})

ggg[ggg.id_series==216265]

np.std(db_voli[db_voli.series == 216265].time)


not_reg = []
for ser in db_slot.id:
    times = db_voli[db_voli.series == ser]["time"]
    mean = np.mean(times)
    vars = np.std(times)
    if vars > 200:
        not_reg.append(ser)

for i in not_reg[:20]:
    print(db_voli[db_voli.series == i])



db_voli_copy = db_voli.copy()

fixed = []
dd = []
for ser in not_reg:
    times = db_voli[db_voli.series == ser]["time"].copy()
    if times[times>= 1260].shape[0] >= times[times <= 180].shape[0]:
        times[times <= 180] = times[times <= 180] + 1440
    else:
        times[times >= 1260] = times[times >= 1260] - 1440

    mean = np.mean(times)
    vars = np.std(times)
    if  vars > 200:
        dd.append(ser)
        print(ser)
        print(times)
        print(" ")
    fixed.append(times)

    mean = np.mean(times)
len(fixed)
len(not_reg)


bb = db_voli[db_voli.series.isin(dd)]
bb

matched = db_voli[db_voli.match.isin(dd)]

db_slot[db_slot.id==220381]

"""


"""
not regular
"""

"""
departures_2019.callsign.unique().shape

not_regular = departures_2019[
    (~departures_2019.series.isin(series_2019_departures.series)) & (departures_2019["week day"] == 0)]

date_num = dict(zip(np.sort(series_2019_departures.day.unique()), range(len(series_2019_departures.day.unique()))))

columns = ['icao24', 'departure', 'arrival', 'callsign', 'day', 'week day', 'series code', 'series',
           'airline', 'is_departure', 'mean_time']
df_double = pd.DataFrame(columns=columns)
for i in not_regular.callsign.unique():
    ddf = not_regular[not_regular.callsign == i]
    mean = ddf["dep time minute"].mean()
    above = ddf[ddf["dep time minute"] > mean + 60]

    above_mean = above["dep time minute"].mean()
    # print(mean)
    # print(above)
    under = ddf[ddf["dep time minute"] < mean - 60]
    under_mean = under["dep time minute"].mean()
    # print(under)

    if under.shape[0] > 4 and above.shape[0] > 4:
        if above[(above["dep time minute"] < above_mean + 60) & (above["dep time minute"] > above_mean - 60)].shape[0] / \
                above.shape[
                    0] > 0.7:
            to_append = above[columns[:-2]].iloc[0].to_list() + [True] + [above_mean]
            df_double = df_double.append(dict(zip(columns, to_append)), ignore_index=True)

        if under[(under["dep time minute"] < under_mean + 60) & (under["dep time minute"] > under_mean - 60)].shape[0] / \
                under.shape[
                    0] > 0.7:
            to_append = under[columns[:-2]].iloc[0].to_list() + [True] + [under_mean]
            df_double = df_double.append(dict(zip(columns, to_append)), ignore_index=True)

df_not_double = not_regular[~not_regular.callsign.isin(df_double.callsign)]
df_not_double.callsign.unique().shape
df_double

f.plot_series(not_regular, df_not_double.callsign.unique()[:70])


gf.to_csv("data/test_definitive/test_gf.csv", index_label=False, index=False, sep="\t")

def get_new_entries(gf, next_year_series, departure):
    new_entries = pd.DataFrame(columns=["airport", "airline"])
    for airport in gf.airport.unique():
        dep_arr = "departure" if departure else "arrival"
        df_a = next_year_series[next_year_series[dep_arr] == airport]
        in_gf = gf[gf.airport == airport].airline.to_list()
        new = df_a[~df_a.airline.isin(in_gf)].airline.unique()
        new_entries = pd.concat([new_entries, pd.DataFrame(
            {"airport": [airport] * len(new), "airline": new})], ignore_index=True)
    return new_entries


new_entries = get_new_entries(gf, first_day, True)
new_entries.to_csv("data/test_definitive/new_entries.csv", index_label=False, index=False, sep="\t")

num_airlines = gf.airline.unique().shape[0]

"""
