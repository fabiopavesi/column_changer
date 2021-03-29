# Configurazione iniziale

Il sistema è pensato per funzionare su MySQL/MariaDB.

I dati per la connessione vanno impostati nel file config.py nella root di questo repository.
Un esempio di configurazione è presente in [config.py.sample](config.py.sample)

L'elenco dei campi da cambiare va messo in un file data.csv, di cui è riportato un esempio in [data.csv.sample](data.csv.sample).

Le classi di modifiche da applicare sono espresse in [db/table.py](db/table.py) alla riga 5.
# Test

Il calcolo dell'errore, su una tabella contenente C campi il cui tipo è
cambiato ed N righe, usa la formula ![alt text](./doc/readme.png).
