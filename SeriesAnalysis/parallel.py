air_size_dict = {}
for airline in df_eu_day.airline.unique():
    air_size_dict[airline] = df_eu_day[df_eu_day.airline == airline].shape[0]

air_size_dict = {k: v for k, v in sorted(air_size_dict.items(), key=lambda item: item[1])}

p = df_eu_day.airline.value_counts()
p.name