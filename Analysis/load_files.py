import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}


def load_slot():
    slot_list_18, slot_list_19 = [], []
    for week_day in range(7):
        slot_18 = pd.read_csv("results/18/slot_2018_" + day_dict[week_day] + ".txt", sep="\t")
        slot_18["week day"] = [week_day for _ in range(slot_18.shape[0])]
        slot_list_18.append(slot_18)

        slot_19 = pd.read_csv("results/19/" + day_dict[week_day] + "/db_slot_" + day_dict[week_day] + ".txt", sep="\t")
        slot_19["week day"] = [week_day for _ in range(slot_19.shape[0])]
        slot_list_19.append(slot_19)

    return pd.concat(slot_list_18, ignore_index=True), pd.concat(slot_list_19, ignore_index=True)


def load_voli():
    list_voli_18, list_voli_19 = [], []
    for week_day in range(7):
        day_voli_18 = pd.read_csv("results/18/voli_2018_" + day_dict[week_day] + ".txt", sep="\t")
        day_voli_18["week day"] = [week_day for _ in range(day_voli_18.shape[0])]
        list_voli_18.append(day_voli_18)

        day_voli_19 = pd.read_csv("results/19/" + day_dict[week_day] + "/db_voli_" + day_dict[week_day] + ".txt",
                                  sep="\t")
        day_voli_19["week day"] = [week_day for _ in range(day_voli_19.shape[0])]
        list_voli_19.append(day_voli_19)

    return pd.concat(list_voli_18, ignore_index=True), pd.concat(list_voli_19, ignore_index=True)


def load_gf_new_entrant():
    gf_list, new_list = [], []
    for week_day in range(7):
        gf = pd.read_csv("results/19/" + day_dict[week_day] + "/gf_" + day_dict[week_day] + ".txt",
                                  sep="\t")
        gf["week day"] = [week_day for _ in range(gf.shape[0])]
        gf_list.append(gf)

        new_entrant = pd.read_csv("results/19/" + day_dict[week_day] + "/new_entrant_" + day_dict[week_day] + ".txt",
                                  sep="\t")
        new_entrant["week day"] = [week_day for _ in range(new_entrant.shape[0])]
        new_list.append(new_entrant)

    return pd.concat(gf_list, ignore_index=True), pd.concat(new_list, ignore_index=True)
