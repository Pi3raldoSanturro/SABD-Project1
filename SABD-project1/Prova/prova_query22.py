import pandas as pd

# Carica il dataset CSV
df = pd.read_csv('../dataset/dataset.csv')

# Filtra solo i record con guasti (failure == 1)
df_failures = df[df['failure'] == 1]

# Raggruppa per vault_id e conta i guasti
vault_failures_count = df_failures.groupby('vault_id').size().reset_index(name='failure_count')

# Ordina i vault_id in base al numero di guasti in ordine decrescente
top_vault_failures = vault_failures_count.sort_values(by='failure_count', ascending=False).head(10)

# Inizializza un dizionario per memorizzare i modelli soggetti a fallimenti per ogni vault_id
vault_models = {}

# Itera sui top 10 vault_id e raccoglie i modelli di hard disk soggetti a fallimenti
for vault_id in top_vault_failures['vault_id']:
    # Filtra i record per il vault_id corrente
    df_vault = df_failures[df_failures['vault_id'] == vault_id]
    # Ottieni i modelli di hard disk soggetti a fallimenti senza ripetizioni
    unique_models = df_vault['model'].unique()
    # Aggiungi al dizionario
    vault_models[vault_id] = unique_models

# Stampa la classifica dei top 10 vault_id con il numero di fallimenti e i modelli soggetti a fallimenti
for index, row in top_vault_failures.iterrows():
    vault_id = row['vault_id']
    failures = row['failure_count']
    models = ", ".join(vault_models[vault_id])
    print(f"Vault ID: {vault_id}, Fallimenti: {failures}, Modelli: {models}")

