import csv
import pandas as pd

day_dict = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}


def make_struttura():

    compagnie = []
    aeroporti = []
    cap = []

    with open('Struttura/1_compagnie.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            compagnie.append(row)

    with open('Struttura/2_aeroporti.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            aeroporti.append(row)

    with open('Struttura/3_cap_osservata.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            cap.append(row)

    for day in day_dict.keys():

        db_slot = pd.read_csv('results/19/'+day_dict[day]+'/db_slot_'+day_dict[day]+'.txt', sep="\t")
        airlines = list(db_slot.Airline.unique())

        gf = []
        with open('results/19/'+day_dict[day]+'/gf_'+day_dict[day]+'.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            for row in csv_reader:
                gf.append(row)

        new_entrant = []
        with open('results/19/'+day_dict[day]+'/new_entrant_'+day_dict[day]+'.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            for row in csv_reader:
                new_entrant.append(row)



        with open('results/19/'+day_dict[day]+'/struttura'+day_dict[day]+'.txt', mode='w') as struttura:
            writer = csv.writer(struttura, delimiter='\t', escapechar='\t', quoting=csv.QUOTE_NONE)
            for row in compagnie:
                writer.writerow(row)

            writer.writerow([db_slot.Airline.unique().shape[0]])

            for row in aeroporti:
                writer.writerow(row)

            writer.writerow(["compagnie"])
            writer.writerow(airlines)

            writer.writerow(["newentrant"])
            for row in new_entrant[1:]:
                writer.writerow(row)

            writer.writerow(["grandFather"])
            for row in gf[1:]:
                writer.writerow(row)

            for row in cap:
                writer.writerow(row)

