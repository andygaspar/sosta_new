import pandas as pd
import numpy as np

db_v = pd.read_csv("../DBvoli_Friday.txt", sep="\t")

match = db_v[db_v.match != -1]
match = db_v.drop_duplicates(subset="match")
non_macth = db_v[db_v.match == -1]

db_voli = pd.concat([match, non_macth], ignore_index=True)
db_voli = db_voli.drop_duplicates()

db_s = pd.read_csv("../DBslot_Friday.txt", sep="\t")

time = db_s.Time.apply(lambda t: int(t / 10) * 10 if (t // 5) % 2 == 0 else int(t / 10) * 10 + 5).astype(int)
db_s.Time = time

matched = db_s[db_s.matched != -1].id

for id in matched:
    dep = db_s[db_s.id == id].iloc[0]
    m = dep.matched
    arr = db_s[db_s.id == m].iloc[0]
    if dep.Time > arr.Time:
        print(dep, arr)
