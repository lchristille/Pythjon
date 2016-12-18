import sqlite3 as lite
import sys
from couchbase.bucket import Bucket

con = None
cb = Bucket('couchbase://localhost/bibbiacei2008')
cb.flush()
try:
    con = lite.connect('dbBibbia.sqlite')

    cur = con.cursor()
    cur.execute('SELECT * FROM TESTI')

    rows = cur.fetchall()

    for row in rows:
        # print (row)
        # Interroga SQLite per trovare il numero di capitolo del versetto
        cur2 = con.cursor()
        cur2.execute(str("SELECT _id, IDLIBRO, CAPITOLO FROM CAPITOLI WHERE _id LIKE '" + str(row[1]) + "'"))
        capitolo = cur2.fetchone()

        if str(capitolo[2]) == "0":
            if capitolo[1] and capitolo [2]:
                idlibro = capitolo[1]
                capitolo = capitolo[2]

            # Dal numero del libro, interroga il database per trovare il nome del libro
            cur3 = con.cursor()
            cur3.execute(str("SELECT ID, LIBRO FROM LIBRI WHERE ID LIKE '" + str(idlibro) + "'"))
            nomeLibro = cur3.fetchone()
            nomeLibro = nomeLibro[1]

            # Estrae dalla risposta del database il testo del versetto
            descrizione = str(row[2])
            print(nomeLibro + " " + descrizione)
            cb.upsert('d:' + str(nomeLibro), {'type': 'descrizione', 'libro': str(nomeLibro), 'testo': str(descrizione)})

        else:
            if capitolo[1] and capitolo [2]:
                idlibro = capitolo[1]
                capitolo = capitolo[2]

            # Dal numero del libro, interroga il database per trovare il nome del libro
            cur3 = con.cursor()
            cur3.execute(str("SELECT ID, LIBRO FROM LIBRI WHERE ID LIKE '" + str(idlibro) + "'"))
            nomeLibro = cur3.fetchone()
            nomeLibro = nomeLibro[1]

            # Estrae dalla risposta del database il testo del versetto
            versetto = str(row[2])
            versetto = versetto.split(">")
            # Dal testo del versetto diviso da > estrae il numero del versetto
            numVersetto = str(versetto[2][0:-3])
            # Dal testo del versetto diviso da > estrae il versetto vero e proprio
            versetto = str(versetto[4])
            if versetto.endswith("<br"):
                versetto = versetto[0:-3]
                print(" _id: " + str(row[0]) + " " + nomeLibro + " " + capitolo + ", " + numVersetto + ": " + versetto)
                print ("")
                cb.upsert("t:" + str(nomeLibro) + "_" + str(capitolo) + "_" + str(numVersetto),
                          {'type': 'contenuto', 'libro': str(nomeLibro), 'capitolo': str(capitolo), 'versetto': str(numVersetto),
                           'testo': str(versetto), 'fineParagrafo': 'true' })
            else:
                print(" _id: " + str(row[0]) + " " + nomeLibro + " " + capitolo + ", " + numVersetto + ": " + versetto)
                cb.upsert("t:" + str(nomeLibro) + "_" + str(capitolo) + "_" + str(numVersetto),
                          {'type': 'contenuto', 'libro': str(nomeLibro), 'capitolo': str(capitolo), 'versetto': str(numVersetto),
                           'testo': str(versetto), 'fineParagrafo': 'false'})

except lite.Error as e:
    print("Error %s: " % e.args[0])
    sys.exit(1)

finally:
    if con:
        con.close()