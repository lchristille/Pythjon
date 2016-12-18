from couchbase.bucket import Bucket
from couchbase.exceptions import NotFoundError

cb = Bucket('couchbase://localhost/bibbiacei2008')

for row in cb.n1ql_query("SELECT libro FROM bibbiacei2008 WHERE type = 'descrizione'"):
    for capitolo in range(160):
        for numVersetto in range(400):
            searchID = 't:' + str(row['libro']) + '_' + str(capitolo) + '_' + str(numVersetto)
            try:
                cb.get(searchID)
                print(searchID)
            except NotFoundError as e:
                print("", end="")