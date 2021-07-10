import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df_eu = pd.read_csv("DataRefactor/Analisi_friday/europe_2019.csv")

df = df_eu[df_eu["week day"] == 5]

db_voli = pd.read_csv("DataRefactor/Analisi_friday/ok/DBvoli_Friday.txt", sep="\t")


db_slot = pd.read_csv("DataRefactor/Analisi_friday/ok/DBslot_Friday.txt", sep="\t")

len_series = db_slot[["id","InitialDate","FinalDate"]].copy()

len_series["len"] = len_series.FinalDate - len_series.InitialDate + 1

lens = sorted(len_series.len.unique())[1:]

len_num_dict = {}
for l in lens:
    len_num_dict[l] = len_series[len_series.len == l].shape[0]

plt.bar(len_num_dict.keys(), len_num_dict.values())