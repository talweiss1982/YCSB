package site.ycsb.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.ravendb.client.documents.*;
import net.ravendb.client.documents.operations.GetStatisticsOperation;
import net.ravendb.client.documents.session.IDocumentSession;
import net.ravendb.client.exceptions.ConcurrencyException;
import net.ravendb.client.exceptions.database.DatabaseDoesNotExistException;
import net.ravendb.client.serverwide.DatabaseRecord;
import net.ravendb.client.serverwide.operations.CreateDatabaseOperation;
import site.ycsb.*;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RavenDB binding for YCSB framework.
 */
public class RavenDBClient extends DB {
  private String serverUrl;
  private static String defaultServerUrl = "http://localhost:8080";
  private static String defaultDatabaseName = "ycsb";
  private String database;
  private ObjectMapper objectMapper = new ObjectMapper();
  private static  IDocumentStore store;
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    if (store == null) {
      synchronized (RavenDBClient.class) {
        if (store == null) {
          store = createStore();
        }
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        store.close();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }
  private IDocumentStore createStore() {
    Properties props = getProperties();
    serverUrl = props.getProperty("url", defaultServerUrl);
    database = props.getProperty("database", defaultDatabaseName);
    store = new DocumentStore(serverUrl, database);
    store.initialize();
    ensureDatabaseCreated();
    return store;
  }

  private void ensureDatabaseCreated() {
    try {
      store.maintenance().forDatabase(database).send(new GetStatisticsOperation());
    } catch (DatabaseDoesNotExistException dde) {
      try {
        store.maintenance().server().send(new CreateDatabaseOperation(new DatabaseRecord(database)));
      } catch (ConcurrencyException ce) {
        // The database was already created before calling CreateDatabaseOperation
      }
    }
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
      ObjectNode document = generateJsonFromFields(values, table);
      session.store(document, key);
      session.saveChanges();
      return Status.OK;
    }
  }

  private ObjectNode generateJsonFromFields(Map<String, ByteIterator> values, String table) {
    ObjectNode document = objectMapper.createObjectNode();
    for(Map.Entry<String, ByteIterator> entry : values.entrySet()){
      document.put(entry.getKey(), entry.getValue().toArray());
    }
    ObjectNode metadata = objectMapper.createObjectNode();
    metadata.put("@collection", table);
    document.set("@metadata", metadata);
    return document;
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
