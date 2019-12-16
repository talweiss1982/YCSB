package site.ycsb.db;

import net.ravendb.client.documents.*;
import net.ravendb.client.documents.session.IDocumentSession;
import site.ycsb.*;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
/**
 * RavenDB binding for YCSB framework.
 */
public class RavenDBClient extends DB {
  private String serverUrl;
  private static String defaultServerUrl = "http://localhost:8080";
  private static String defaultDatabaseName = "";
  private String database;
  private static  IDocumentStore store;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    serverUrl = props.getProperty("url", defaultServerUrl);
    database = props.getProperty("database", defaultDatabaseName);
    getStoreInstance(serverUrl, database);
  }

  private IDocumentStore getStoreInstance(String ravendbUrl, String dbName) {
    if (store == null) {
      synchronized (RavenDBClient.class) {
        if (store == null) {
          store = createStore(ravendbUrl, dbName);
        }
      }
    }
    return store;

  }

  private IDocumentStore createStore(String ravendbUrl, String dbName) {
    IDocumentStore documentStore = new DocumentStore(ravendbUrl, dbName);
    documentStore.initialize();
    return documentStore;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try (IDocumentSession session = store.openSession()) {
      ObjectNode document = session.load(ObjectNode.class, key);
      populateFieldsForSingleDocument(fields, result, document);
    }
    return Status.OK;
  }

  private void populateFieldsForSingleDocument(Set<String> fields, Map<String, ByteIterator> result,
                                               ObjectNode document) {
    if(fields == null){
      document.fields().forEachRemaining(e-> result.put(e.getKey(),
          new ByteArrayByteIterator(e.getValue().toString().getBytes())));
    } else {
      for (String field: fields){
        //TODO:see if we can have an array pool of ByteArrayByteIterator
        //TODO:see if we can extract the bytes in a cheaper way
        result.put(field, new ByteArrayByteIterator(document.get(field).toString().getBytes()));
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (IDocumentSession session = store.openSession()) {
      //RavenDB scan starts after the given key so we need to fetch the first key separately but we
      // can do this in one round trip.
      Lazy<ObjectNode> first =  session.advanced().lazily().load(ObjectNode.class, startkey);
      Lazy<Map<String, ObjectNode>> rest = session.advanced().lazily().loadStartingWith(
          ObjectNode.class, null, null, 0, recordcount-1, null, startkey);
      session.advanced().eagerly().executeAllPendingLazyOperations();
      HashMap<String, ByteIterator> map = new HashMap<>();
      populateFieldsForSingleDocument(fields, map , first.getValue());
      result.add(map);
      //RavenDB client returns a Map we must sort the keys to get them in the right order.
      Map<String, ObjectNode> documents = rest.getValue();
      List<String> sortedKeys = new ArrayList(documents.keySet());
      Collections.sort(sortedKeys);
      for (String key : sortedKeys) {
        map = new HashMap<>();
        populateFieldsForSingleDocument(fields, map , documents.get(key));
        result.add(map);
      }
      return Status.OK;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try (IDocumentSession session = store.openSession()) {
      ObjectNode document = session.load(ObjectNode.class, key);
      for(Map.Entry<String, ByteIterator> entry : values.entrySet()){
        document.put(entry.getKey(), entry.getValue().toArray());
      }
      session.saveChanges();
      return Status.OK;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try (IDocumentSession session = store.openSession()) {
      session.store(values, key);
      session.saveChanges();
      return Status.OK;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try (IDocumentSession session = store.openSession()) {
      session.delete(key);
      session.saveChanges();
      return Status.OK;
    }
  }

}
