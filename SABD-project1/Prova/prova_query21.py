import pandas as pd

# Carica il dataset CSV
df = pd.read_csv('../dataset/dataset.csv')

# Filtra solo i record con guasti (failure == 1)
df_failures = df[df['failure'] == 1]

# Raggruppa per modello e conta i guasti
failures_count = df_failures.groupby('model').size().reset_index(name='failure_count')

# Ordina i modelli in base al numero di guasti in ordine decrescente
top_failures = failures_count.sort_values(by='failure_count', ascending=False).head(10)

# Mostra la classifica dei 10 modelli con più guasti
print(top_failures)



