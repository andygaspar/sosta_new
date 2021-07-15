import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
# df = pd.read_csv('data/series_2019.csv')
airports = pd.read_csv('data/airports.csv')
europe_codes = np.unique([air[:2] for air in airports.airport])

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", }
day = 0


start_minute = np.array([0, 540, 900, 1140])
end_minute = np.array([539, 899, 1139, 1440])
print(airports.airport)
for day in range(7):
    db_voli = pd.read_csv("data/final/DBvoli_" + day_dict[day] + ".txt", sep="\t")
    gf = pd.read_csv("data/final/gf_" + day_dict[day] + ".txt", sep="\t")
    new_entrant = pd.DataFrame(columns=["airport", "airline", "day"])
    for airport in airports.airport:
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
                        new_entrant = new_entrant.append({"airport": airport, "airline": airline, "day": d},
                                                         ignore_index=True)
                        found = True
                    else:
                        gf_slots = g.slots.values[0]
                        val = airp_day_airl_db[(start_minute[i] <= airp_day_airl_db.time) &
                                               (airp_day_airl_db.time < end_minute[i])].shape[0]
                        if val > gf_slots:
                            new_entrant = new_entrant.append({"airport": airport, "airline": airline, "day": d},
                                                             ignore_index=True)
                            found = True
                    i += 1
    print("day ", day)

    new_entrant.to_csv("data/final/new_entrant_" + day_dict[day] + ".txt", index_label=False, index=False, sep="\t")