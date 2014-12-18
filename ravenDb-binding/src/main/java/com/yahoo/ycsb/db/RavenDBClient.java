/**
 * Created by Tal on 12/16/2014.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import net.ravendb.abstractions.basic.CloseableIterator;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.document.BulkInsertOperation;
import net.ravendb.client.document.DocumentStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class RavenDBClient extends DB {
    @Override
    public void init() throws DBException {
    }
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        JsonDocument obj = getStoreInstance().getDatabaseCommands().get(key);
        for (String field : fields) {
            result.put(field, new StringByteIterator(obj.getDataAsJson().get(field).toString()));
        }
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        CloseableIterator<RavenJObject> iterator = getStoreInstance().getDatabaseCommands().streamDocs(null, startkey, null, 0, recordcount);
        while (iterator.hasNext()) {
            HashMap<String, ByteIterator> docFields = new HashMap<>();
            RavenJObject document = iterator.next();
            for (String field : fields) {
                docFields.put(field, new StringByteIterator(document.get(field).toString()));
            }
            result.add(docFields);
        }
        iterator.close();
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        IDatabaseCommands databaseCommands = getStoreInstance().getDatabaseCommands();
        JsonDocument obj = databaseCommands.get(key);
        RavenJObject document = obj.getDataAsJson();
        for (Map.Entry<String,ByteIterator> entry : values.entrySet())
        {
            document.add(entry.getKey(),entry.getValue().toString());
        }
        databaseCommands.put(key, null, document, obj.getMetadata());
        return 0;
    }


    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        RavenJObject insertMetadata = new RavenJObject();
        insertMetadata.add("Raven-Entity-Name", "Users");

        RavenJObject obj = new RavenJObject();
        for (Map.Entry<String,ByteIterator> entry : values.entrySet())
        {
            obj.add(entry.getKey(),entry.getValue().toString());
        }
        try {
            getBulkOperation().store(obj, insertMetadata, key);
        } catch (Exception e) {
            return -1;
        }
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println("delete, Table:" + table + " " +"Key:" + key);
        return 0;
    }
    @Override
    public void cleanup() throws DBException {
        try {
            getBulkOperation().close();
        } catch (Exception e){

        }
    }
    public static void main(String[] args)
    {
        RavenDBClient client = new RavenDBClient();
        try {
            client.init();
        } catch (DBException e)
        {
            String message = e.getMessage();
            System.out.print(message);
            System.out.print("oh crap!");
        }
    }

    public static IDocumentStore getStoreInstance() {
        return store;
    }

    private static IDocumentStore createStore() {
        IDocumentStore store = new DocumentStore("http://localhost:8080", "Test");
        store.initialize();
        return store;
    }

    public BulkInsertOperation getBulkOperation()
    {
        if (bulkOperation == null)
            bulkOperation = store.bulkInsert();
        return bulkOperation;
    }

    private BulkInsertOperation bulkOperation;
    private static IDocumentStore store = createStore();
}
