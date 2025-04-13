## Coding Conventions

- Always use **double quotes** for strings.
- Use structured logging instead of `System.out.println()` and `System.err.println()`.
- Use builder pattern and strongly typed parameters when interacting with the client.

## Initialization

All code must use the **OpenIAP Java client library**. Start by loading the native library, initializing the client, and connecting:

```java
String libpath = NativeLoader.loadLibrary("openiap");
Client client = new Client(libpath);
client.start();
client.connect("");
```

## Connection Lifecycle

If your code needs to reinitialize state when reconnecting (e.g., registering a queue or setting up a watch), register a client event listener:

```java
client.onClientEventAsync((event) -> {
    if ("SignedIn".equals(event.event)) {
        onConnected();
    }
});

void onConnected() {
    // e.g., registerQueueAsync, watchAsync, etc.
}
```

## API Reference

### Authentication
```java
client.connect("");
client.signin(new SigninParameters.Builder()
    .username(System.getenv("username"))
    .password(System.getenv("password"))
    .build());
client.close();
```

### Database Operations
```java
client.query(TypeReference<List<Entity>>, new QueryParameters.Builder()
    .collectionname("entities")
    .query("{\"_type\":\"test\"}")
    .top(10)
    .build());

client.aggregate(TypeReference<List<Entity>>, new AggregateParameters.Builder()
    .collectionname("entities")
    .aggregates("[...]")
    .build());

client.count(new CountParameters.Builder()
    .collectionname("entities")
    .query("{\"_type\":\"test\"}")
    .build());

client.distinct(new DistinctParameters.Builder()
    .collectionname("entities")
    .field("_type")
    .build());

client.insertOne(new InsertOneParameters.Builder()
    .collectionname("entities")
    .item("{\"_type\":\"test\", \"name\":\"test01\"}")
    .build());

client.insertMany(new InsertManyParameters.Builder()
    .collectionname("entities")
    .items("[...]")
    .build());

client.updateOne(new UpdateOneParameters.Builder()
    .collectionname("entities")
    .item("{...}")
    .build());

client.insertOrUpdateOne(new InsertOrUpdateOneParameters.Builder()
    .collectionname("entities")
    .item("{...}")
    .uniqeness("name")
    .build());

client.deleteOne(new DeleteOneParameters.Builder()
    .collectionname("entities")
    .id("...")
    .build());

client.deleteMany(new DeleteManyParameters.Builder()
    .collectionname("entities")
    .query("{...}")
    .build(), null);
```

### Collection & Index Management
```java
client.listCollections(false);
client.createCollection(new CreateCollection.Builder("collectionname").build());
client.dropCollection("collectionname");
client.getIndexes("collectionname");
client.createIndex(new CreateIndexParameters.Builder()
    .collectionname("collectionname")
    .index("{\"field\":1}")
    .build());
client.dropIndex("collectionname", "indexname");
```

### File Transfer
```java
client.upload(new UploadParameters.Builder()
    .filepath("testfile.csv")
    .filename("testfile.csv")
    .collectionname("fs.files")
    .build());

client.download(new DownloadParameters.Builder()
    .collectionname("fs.files")
    .id("fileid")
    .filename("output.csv")
    .build());
```

### Events & Messaging
```java
client.registerQueueAsync(new RegisterQueueParameters.Builder()
    .queuename("myqueue")
    .build(), (eventPtr) -> {
        QueueEvent event = ...;
        // handle event
        return "response";
    });

client.unregisterQueue("myqueue");

client.registerExchangeAsync(new RegisterExchangeParameters.Builder()
    .exchangename("myexchange")
    .algorithm("fanout")
    .addqueue(true)
    .build(), (event) -> {
        // handle exchange event
    });

client.queueMessage(new QueueMessageParameters.Builder()
    .queuename("myqueue")
    .message("{\"key\":\"value\"}")
    .striptoken(true)
    .build());

client.watchAsync(new WatchParameters.Builder()
    .collectionname("entities")
    .build(), (event) -> {
        // handle change stream
    });

client.unwatch("watchid");
```

### Observability & Logging
```java
client.enableTracing("openiap=trace", "");
client.disableTracing();

client.info("Log message");
client.error("Error message");
client.verbose("Verbose output");
client.trace("Trace info");

client.setF64ObservableGauge("metric.name", 42.0, "metric description");
client.setU64ObservableGauge("metric.name", 10, "...");
client.setI64ObservableGauge("metric.name", -1, "...");
client.disableObservableGauge("metric.name");
```

---

## Example Pattern

This is how to register a queue and process messages:

```java
client.registerQueueAsync(new RegisterQueueParameters.Builder()
    .queuename("myqueue")
    .build(), (eventPtr) -> {
        QueueEvent event = new QueueEvent();
        // deserialize and process
        client.info("Received message: " + event.getData());
        return "{"response": "ok"}";
    });
```

This is how to handle watch events:

```java
client.watchAsync(new WatchParameters.Builder()
    .collectionname("entities")
    .build(), (event) -> {
        client.info("Change detected: " + event.operation);
    });
```

