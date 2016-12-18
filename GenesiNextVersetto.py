from couchbase.bucket import Bucket
from couchbase.exceptions import NotFoundError
from couchbase.exceptions import CouchbaseTransientError
from couchbase.n1ql import N1QLQuery

cb = Bucket('couchbase://localhost/bibbiacei2008')
class ID:
    book = ""
    chapter = ""
    verse = ""

    def generateID(self):
        return "t:"+ str(self.book) + "_" + str(self.chapter) + "_" + str(self.verse)

    def nextVerse(self):
        nextVerseID = ID()
        nextVerseID.book = self.book
        nextVerseID.chapter = self.chapter
        nextVerseID.verse = str(int(self.verse) + 1)
        return nextVerseID

    def prevVerse(self):
        prevVerseID = ID()
        prevVerseID.book = self.book
        prevVerseID.chapter = self.chapter
        if int(self.verse) > 1:
            prevVerseID.verse = str(int(self.verse) - 1)
            return prevVerseID
        else:
            return

    def nextChapter(self):
        nextChapterID = ID()
        nextChapterID.book = self.book
        nextChapterID.chapter = str(int(self.chapter) + 1)
        nextChapterID.verse = "1"
        return nextChapterID

print_ver2 = False
searchID = ID()
Libri = ["Genesi", "Esodo", "Levitico", "Numeri", "Deuteronomio", "Giosuè", "Giudici", "Rut", "Samuele 1", "Samuele 2",
         "Re 1", "Re 2", "Cronache 1", "Cronache 2", "Esdra", "Neemia", "Tobia", "Giuditta", "Ester", "Maccabei 1",
         "Maccabei 2", "Giobbe", "Salmi", "Proverbi", "Qoelet", "Cantico dei Cantici", "Sapienza", "Siracide", "Isaia",
         "Geremia", "Lamentazioni", "Baruc", "Ezechiele", "Daniele", "Osea", "Gioele", "Amos", "Abdia", "Giona",
         "Michea", "Naum", "Abacuc", "Sofonia", "Aggeo", "Zaccaria", "Malachia", "Marco", "Luca", "Matteo", "Giovanni",
         "Atti degli Apostoli", "Romani", "Corinzi 1", "Corinzi 2", "Galati", "Efesini", "Filippesi", "Colossesi",
         "Tessalonicesi 1", "Tessalonicesi 2", "Timoteo 1", "Timoteo 2", "Tito", "Filemone", "Ebrei", "Giacomo",
         "Pietro 1", "Pietro 2", "Giovanni 1", "Giovanni 2", "Giovanni 3", "Giuda", "Apocalisse"]

Libri2 = ["Ester", "Maccabei 1",
         "Maccabei 2", "Giobbe", "Salmi", "Proverbi", "Qoelet", "Cantico dei Cantici", "Sapienza", "Siracide", "Isaia",
         "Geremia", "Lamentazioni", "Baruc", "Ezechiele", "Daniele", "Osea", "Gioele", "Amos", "Abdia", "Giona",
         "Michea", "Naum", "Abacuc", "Sofonia", "Aggeo", "Zaccaria", "Malachia", "Marco", "Luca", "Matteo", "Giovanni",
         "Atti degli Apostoli", "Romani", "Corinzi 1", "Corinzi 2", "Galati", "Efesini", "Filippesi", "Colossesi",
         "Tessalonicesi 1", "Tessalonicesi 2", "Timoteo 1", "Timoteo 2", "Tito", "Filemone", "Ebrei", "Giacomo",
         "Pietro 1", "Pietro 2", "Giovanni 1", "Giovanni 2", "Giovanni 3", "Giuda", "Apocalisse"]

for libro in Libri2:
    searchID.book = libro
    print ("Elaborazione libro " + libro + " in corso")
    for _chap in range(1,160):
        print(_chap)
        if _chap != 1:
            try:
                searchID.chapter = str(_chap)
                searchID.verse = str(1)
                row = cb.get(searchID.generateID())
                if print_ver2 == True: print(row.value['testo'])
                actVal = row.value
                strQuery = "SELECT * FROM bibbiacei2008 WHERE succID=\"" + str(searchID.generateID() + "\"")
                query = N1QLQuery(strQuery)
                query.adhoc = 'false'
                for row in cb.n1ql_query(query):
                    row = row['bibbiacei2008']
                    precID = "t:"+ str(row['libro']) + "_" + str(row['capitolo']) + "_" + str(row['versetto'])
                    cb.upsert("t:" + actVal['libro'] + "_" + actVal['capitolo'] + "_" + actVal['versetto'],
                              {'type': 'contenuto', 'libro': actVal['libro'], 'capitolo': actVal['capitolo'],
                               'versetto': actVal['versetto'],
                               'testo': actVal['testo'], 'fineParagrafo': actVal['fineParagrafo'],
                               'precID': precID,
                               'succID': str(searchID.nextVerse().generateID())})

            except NotFoundError:
                print("")

        for _ver in range(2,3):
            try:
                searchID.chapter = str(_chap)
                searchID.verse = str(_ver)
                row = cb.get(searchID.generateID())
                if print_ver2 == True: print(row.value['testo'])
                actVal = row.value
                previousID = searchID.prevVerse().generateID()

                cb.upsert("t:" + actVal['libro'] + "_" + actVal['capitolo'] + "_" + actVal['versetto'],
                          {'type': 'contenuto', 'libro': actVal['libro'], 'capitolo': actVal['capitolo'],
                           'versetto': actVal['versetto'],
                           'testo': actVal['testo'], 'fineParagrafo': actVal['fineParagrafo'],
                           'precID': str(previousID)})

                previous = cb.get(previousID)
                if print_ver2 == True: print("Previous: " + str(previous.value))
                prVal = previous.value
                if prVal['versetto'] == str(1):
                    cb.upsert("t:" + prVal['libro'] + "_" + prVal['capitolo'] + "_" + prVal['versetto'],
                              {'type': 'contenuto', 'libro': prVal['libro'], 'capitolo': prVal['capitolo'],
                               'versetto': prVal['versetto'],
                               'testo': prVal['testo'], 'fineParagrafo': prVal['fineParagrafo'],
                               'succID': str(searchID.generateID())})
                else:
                    cb.upsert("t:" + prVal['libro'] + "_" + prVal['capitolo'] + "_" + prVal['versetto'],
                              {'type': 'contenuto', 'libro': prVal['libro'], 'capitolo': prVal['capitolo'],
                               'versetto': prVal['versetto'],
                               'testo': prVal['testo'], 'fineParagrafo': prVal['fineParagrafo'],
                               'precID': prVal['precID'],
                               'succID': str(searchID.generateID())})
            except NotFoundError:
                try:
                    searchID.chapter = str(_chap)
                    searchID.verse = str(_ver - 1)
                    row = cb.get(searchID.generateID())
                    if print_ver2 == True: print(row.value['testo'])
                    actVal = row.value

                    cb.upsert("t:" + actVal['libro'] + "_" + actVal['capitolo'] + "_" + actVal['versetto'],
                              {'type': 'contenuto', 'libro': actVal['libro'], 'capitolo': actVal['capitolo'],
                               'versetto': actVal['versetto'],
                               'testo': actVal['testo'], 'fineParagrafo': actVal['fineParagrafo'],
                               'precID': actVal['precID'],
                               'succID': str(searchID.nextChapter().generateID())})
                    break
                except NotFoundError:
                    break
            except CouchbaseTransientError:
                print("Transient Error")