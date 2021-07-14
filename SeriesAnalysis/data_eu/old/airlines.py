import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
# df = pd.read_csv('data/series_2019.csv')
airports = pd.read_csv('data/airports.csv')
europe_codes = np.unique([air[:2] for air in airports.airport])

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", }
day = 0



for day in range(7):
    db_voli = pd.read_csv("data/final/DBvoli_" + day_dict[day] + ".txt", sep="\t")
    airlines = db_voli.airline.unique()
    string = ""
    for airline in airlines[:-1]:
        string += airline + " "
    string += airlines[-1]
    with open("data/final/airlines" + day_dict[day] + ".txt", mode='w') as air_txt:
        air_txt.write(string)
        air_txt.close()