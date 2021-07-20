import pandas as pd
import numpy as np

# gf

gf = pd.read_csv("ok/gf_test.csv", sep="\t")
gf_old = pd.read_csv("ok_test/gf_test.csv", sep="\t")
print(gf.equals(gf_old))

# slot

slot_18 = pd.read_csv("ok/slot_2018.csv", sep="\t")
slot_18_old = pd.read_csv("ok_test/slot_2018.csv", sep="\t")
print(slot_18.equals(slot_18_old))

slot_19 = pd.read_csv("ok/slot_2019.csv", sep="\t")
slot_19_old = pd.read_csv("ok_test/slot_2019.csv", sep="\t")
print(slot_19.equals(slot_19_old))


# voli

voli_18 = pd.read_csv("ok/voli_2018.csv", sep="\t")
voli_18_old = pd.read_csv("ok_test/voli_2018.csv", sep="\t")
print(voli_18 .equals(voli_18_old))

voli_19 = pd.read_csv("ok/voli_2019.csv", sep="\t")
voli_19_old = pd.read_csv("ok_test/voli_2019.csv", sep="\t")
print(voli_19.equals(voli_19_old))
