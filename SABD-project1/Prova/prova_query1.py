import pandas as pd

# Carica il dataset CSV
df = pd.read_csv('../dataset/dataset.csv')

# Filtra solo i record con guasti (failure == 1)
df_failures = df[df['failure'] == 1]

# Converti la colonna 'date' in formato datetime
df_failures['date'] = pd.to_datetime(df_failures['date'], format='%Y-%m-%dT%H:%M:%S.%f')

# Raggruppa per 'vault_id' e data (solo la parte della data), e conta i guasti
daily_failures = df_failures.groupby(['vault_id', df_failures['date'].dt.date]).size().reset_index(name='failure_count')

# Ordina i risultati per data
daily_failures = daily_failures.sort_values(by='date')

# Filtra per ottenere i vault con esattamente 4, 3 e 2 guasti
failures_4 = daily_failures[daily_failures['failure_count'] == 4]
failures_3 = daily_failures[daily_failures['failure_count'] == 3]
failures_2 = daily_failures[daily_failures['failure_count'] == 2]

# Mostra i risultati
print("Vault con esattamente 4 fallimenti (ordinati per data):")
print(failures_4)

print("\nVault con esattamente 3 fallimenti (ordinati per data):")
print(failures_3)

print("\nVault con esattamente 2 fallimenti (ordinati per data):")
print(failures_2)

