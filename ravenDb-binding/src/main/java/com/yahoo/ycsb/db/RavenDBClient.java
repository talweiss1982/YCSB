/**
 * Created by Tal on 12/16/2014.
 */
package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import net.ravendb.abstractions.basic.CloseableIterator;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.converters.ITypeConverter;
import net.ravendb.client.document.BulkInsertOperation;
import net.ravendb.client.document.DocumentStore;

public class RavenDBClient extends DB {
    @Override
    public void init() throws DBException {
        getStoreInstance().getConventions().getIdentityProperty();
    }
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        //System.out.println("Read, Table:" + table + " " +"Key:" + key);
        IDocumentSession session = openSession();
        RavenJObject obj = session.load(RavenJObject.class, key);
        for (String field : fields) {
            result.put(field, new ByteArrayByteIterator(obj.get(field).toString().getBytes()));
        }
        session.saveChanges();
        closeSession(session);
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        //System.out.println("Scan, Table:" + table + " " +"StartKey:" + startkey);
        CloseableIterator<RavenJObject> iterator = getStoreInstance().getDatabaseCommands().streamDocs(null,null,null,Integer.parseInt(startkey.substring("user".length())),recordcount);
        while (iterator.hasNext()) {
            HashMap<String, ByteIterator> docFields = new HashMap<String, ByteIterator>();
            RavenJObject document = iterator.next();
            for (String field : fields) {
                docFields.put(field, new ByteArrayByteIterator(document.get(field).toString().getBytes()));
            }
            result.add(docFields);
        }
        iterator.close();
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        //System.out.println("update, Table:" + table + " " +"Key:" + key);
        IDocumentSession session = openSession();
        RavenJObject document = session.load(RavenJObject.class,key);
        for (Map.Entry<String,ByteIterator> entry : values.entrySet())
        {
            document.add(entry.getKey(),entry.getValue().toArray());
        }
        session.saveChanges();
        closeSession(session);
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        //System.out.println("insert, Table:" + table + " " +"Key:" + key);
        //IDocumentSession session = openSession();
        RavenJObject obj = new RavenJObject();
        for (Map.Entry<String,ByteIterator> entry : values.entrySet())
        {
            obj.add(entry.getKey(),entry.getValue().toArray());
        }
        //session.store(obj);
        //session.saveChanges();
        //closeSession(session);
        try {
            getBulkOperation().store(obj);
        } catch (Exception e) {}
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
        } catch (Exception e){}
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
        if (store == null) {
            synchronized (RavenDBClient.class) {
                if (store == null) {
                    store = createStore();
                }
            }
        }
        return store;
    }
    private  static void closeSession(IDocumentSession session)
    {
        try
        {
            session.close();
        } catch (Exception e) {

        }
    }
    private static IDocumentStore createStore() {
        IDocumentStore store = new DocumentStore("http://localhost:8080", "Test");
        store.initialize();
        return store;
    }

    public static IDocumentSession openSession()
    {
        return getStoreInstance().openSession();
    }

    public BulkInsertOperation getBulkOperation()
    {
        if (bulkOperation == null) bulkOperation = store.bulkInsert();
        return bulkOperation;
    }

    private BulkInsertOperation bulkOperation;
    private static IDocumentStore store;
}
