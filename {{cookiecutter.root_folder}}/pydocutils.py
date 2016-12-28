from pydocumentdb import document_client

def getDatabaseCollectionClient(host, key, dbName, collectionName):
    client = document_client.DocumentClient(host, {'masterKey': key})

    # get or create database
    db = [d for d in client.ReadDatabases() if d['id'] == dbName]
    if len(db) == 0:
        db = client.CreateDatabase({'id': dbName})
    else:
        db = db[0]

    # get or create collection
    col = [col for col in client.ReadCollections(db['_self']) if col['id'] == collectionName]
    if len(col) == 0:
        col = client.CreateCollection(db['_self'], {'id': collectionName})
    else:
        col = col[0]
    return (db, col, client)

def createDocument(host, key, dbName, collectionName, documentJson):
    db, col, client = getDatabaseCollectionClient(host, key, dbName, collectionName)

    # create document
    client.CreateDocument(col['_self'], documentJson)

def upsertDocument(host, key, dbName, collectionName, documentJson):
    db, col, client = getDatabaseCollectionClient(host, key, dbName, collectionName)

    # upsert document
    client.UpsertDocument(col['_self'], documentJson)

def readDocument(host, key, dbName, collectionName, strId):
    db, col, client = getDatabaseCollectionClient(host, key, dbName, collectionName)

    # read document
    doc = [doc for doc in client.ReadDocuments(col['_self']) if doc['id'] == strId]
    if len(doc) == 0:
        return None
    else:
        return doc[0]
