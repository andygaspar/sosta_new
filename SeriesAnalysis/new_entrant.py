import pandas as pd
import numpy as np

start_minute = np.array([0, 540, 900, 1140])
end_minute = np.array([539, 899, 1139, 1439])


def make_new_entrant(airline, air_voli: pd.DataFrame, gf: pd.DataFrame, airport_list):
    columns = ["airport", "airline", "day"]
    new_entrant = pd.DataFrame(columns=columns)
    air_voli = air_voli[air_voli.airport.isin(airport_list)]
    airports = air_voli[air_voli.airport.isin(gf.airport.unique())].airport.unique()
    new_airport = air_voli[~air_voli.airport.isin(gf.airport)].airport.unique()
    for airport in new_airport:
        df_new_airp = air_voli[air_voli.airport == airport]
        days = df_new_airp.day.unique()
        airp = [airport for _ in range(days.shape[0])]
        airs = [airline for _ in range(days.shape[0])]
        new_entrant = pd.concat([new_entrant, pd.DataFrame({"airport": airp, "airline": airs, "day": days})],
                                ignore_index=True)

    for airport in airports:
        v_airp = air_voli[air_voli.airport == airport]
        gf_airp = gf[gf.airport == airport]
        days = v_airp[v_airp.day.isin(gf_airp.day_num)].day.unique()
        new_days = v_airp[~v_airp.day.isin(gf_airp.day_num)].day.unique()
        for d in new_days:
            to_append = [airport, airline, d]
            new_entrant = new_entrant.append(dict(zip(columns, to_append)), ignore_index=True)

        for d in days:
            v_airp_day = v_airp[v_airp.day == d]
            gf_day = gf_airp[gf_airp.day_num == d]
            is_new_entrant = False
            for i in range(4):
                gf_min = gf_day[gf_day.start == start_minute[i]].shape[0]
                num_19_slots = v_airp_day[(v_airp_day.time >= start_minute[i])
                                          & (v_airp_day.time <= end_minute[i])].shape[0]
                num_18_slots = gf_day[gf_day.start == start_minute[i]].slots.values[0] if gf_min > 0 else 0
                if num_19_slots > num_18_slots:
                    is_new_entrant = True
                    break

            if is_new_entrant:
                to_append = [airport, airline, d]
                new_entrant = new_entrant.append(dict(zip(columns, to_append)), ignore_index=True)

    return new_entrant

