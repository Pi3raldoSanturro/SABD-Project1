import pandas as pd
import numpy as np

# Carica il dataset CSV
df = pd.read_csv('../dataset/dataset.csv')

# Filtra solo i record con informazioni sull'ultimo giorno utile di rilevazione per ogni specifico hard disk
df_last_day = df.sort_values(by=['date', 'serial_number']).drop_duplicates(subset='serial_number', keep='last')

# Calcola le statistiche per gli hard disk con fallimenti
df_failures = df_last_day[df_last_day['failure'] == 1]
failures_count = len(df_failures)
failures_min = df_failures['s9_power_on_hours'].min()
failures_25th = np.percentile(df_failures['s9_power_on_hours'], 25)
failures_50th = np.percentile(df_failures['s9_power_on_hours'], 50)
failures_75th = np.percentile(df_failures['s9_power_on_hours'], 75)
failures_max = df_failures['s9_power_on_hours'].max()

# Calcola le statistiche per gli hard disk senza fallimenti
df_no_failures = df_last_day[df_last_day['failure'] == 0]
no_failures_count = len(df_no_failures)
no_failures_min = df_no_failures['s9_power_on_hours'].min()
no_failures_25th = np.percentile(df_no_failures['s9_power_on_hours'], 25)
no_failures_50th = np.percentile(df_no_failures['s9_power_on_hours'], 50)
no_failures_75th = np.percentile(df_no_failures['s9_power_on_hours'], 75)
no_failures_max = df_no_failures['s9_power_on_hours'].max()

# Stampa le statistiche
print("# failure, min, 25th percentile, 50th percentile, 75th percentile, max, count")
print(f"1, {failures_min}, {failures_25th}, {failures_50th}, {failures_75th}, {failures_max}, {failures_count}")
print(f"0, {no_failures_min}, {no_failures_25th}, {no_failures_50th}, {no_failures_75th}, {no_failures_max}, {no_failures_count}")

