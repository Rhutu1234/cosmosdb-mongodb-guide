# Cosmos DB and MongoDB: NoSQL Databases for Flexible Schemas and Scale

*A practical guide to two of the most widely used NoSQL databases — Azure Cosmos DB and MongoDB — covering the document data model, schema flexibility, partitioning/sharding, consistency models, indexing, and how to choose between them (and against a relational database).*

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why NoSQL, and Why Document Databases Specifically](#1-why-nosql-and-why-document-databases-specifically)
3. [The Document Data Model](#2-the-document-data-model)
4. [MongoDB](#3-mongodb)
5. [Azure Cosmos DB](#4-azure-cosmos-db)
6. [Partitioning and Sharding](#5-partitioning-and-sharding)
7. [Consistency Models](#6-consistency-models)
8. [Indexing](#7-indexing)
9. [Data Modeling: Embedding vs. Referencing](#8-data-modeling-embedding-vs-referencing)
10. [Querying](#9-querying)
11. [.NET Integration](#10-net-integration)
12. [Cosmos DB vs. MongoDB vs. a Relational Database](#11-cosmos-db-vs-mongodb-vs-a-relational-database)
13. [Quick Reference Table](#quick-reference-table)
14. [Conclusion](#conclusion)

---

## Introduction

Cosmos DB and MongoDB are both **document databases** — instead of rows in fixed-schema tables, they store data as flexible, JSON-like documents that can vary in shape from one record to the next, and both are built from the ground up for horizontal scale across many machines rather than scaling a single powerful server.

```json
{
  "_id": "42",
  "name": "Ada Lovelace",
  "email": "ada@example.com",
  "addresses": [
    { "type": "home", "city": "London" },
    { "type": "work", "city": "Cambridge" }
  ],
  "preferences": { "newsletter": true, "theme": "dark" }
}
```

That single document — a customer with a variable number of addresses and an open-ended preferences object — would take a join across two or three normalized tables in a relational database. In a document database, it's one record, fetched in one read.

MongoDB is the open-source, self-hostable (or MongoDB Atlas-managed) document database that popularized this model at scale. Azure Cosmos DB is Microsoft's globally distributed, fully managed database service — natively a document database (its "Core (SQL) API"), but also capable of speaking MongoDB's own wire protocol, Cassandra's, Gremlin's (graph), and Azure Table Storage's, all through the same underlying engine.

---

## 1. Why NoSQL, and Why Document Databases Specifically

### Schema flexibility

Relational databases require every row in a table to share the same columns (with `NULL` for anything not applicable). Document databases let each document have its own shape — useful when different records genuinely have different attributes (a product catalog spanning wildly different product types), or when the schema is expected to evolve frequently and you don't want a migration for every new optional field.

```json
{ "type": "book", "title": "...", "author": "...", "isbn": "..." }
{ "type": "laptop", "brand": "...", "cpu": "...", "ramGb": 16 }
```

Both documents can live in the same collection/container without either needing columns the other doesn't use.

### Horizontal scale by design

Where a relational database's default scaling story is "get a bigger server" (vertical scaling), document databases are architected from the start to **shard data across many servers** (horizontal scaling) — a single logical collection can span dozens or hundreds of physical machines, each holding a subset of the data, with the database routing queries to the right shard(s) automatically.

### The tradeoff: less enforced structure, fewer built-in cross-document guarantees

The flexibility document databases provide is also their biggest risk if unmanaged — with no schema enforced by the database itself, inconsistent document shapes can accumulate unnoticed, and multi-document transactional guarantees (while now supported to varying degrees in both systems) are historically weaker and often deliberately avoided for performance reasons, pushing more correctness responsibility onto application-level data modeling discipline (Section 8) than a relational schema and foreign keys would.

---

## 2. The Document Data Model

### Collections/containers instead of tables

- MongoDB groups documents into **collections** within a **database**.
- Cosmos DB groups documents into **containers** within a **database**.

Neither requires documents in the same collection/container to share a fixed schema — though in practice, most real applications keep documents within one collection reasonably consistent in shape, even without the database enforcing it, simply because application code needs to reason about them predictably.

### `_id` as the primary key

```json
{ "_id": ObjectId("507f1f77bcf86cd799439011"), "name": "Wireless Mouse" }
```

Both systems use `_id` as the document's unique identifier. MongoDB defaults to a generated `ObjectId` (a 12-byte value encoding a timestamp and other bits) if you don't supply one; Cosmos DB's Core (SQL) API accepts any string you choose as `id`, and requires it alongside a **partition key** (Section 5) for efficient routing.

### Nested structures are native, not an afterthought

```json
{
  "orderId": "1001",
  "customer": { "name": "Ada Lovelace", "email": "ada@example.com" },
  "items": [
    { "productId": "42", "quantity": 2, "price": 29.99 },
    { "productId": "17", "quantity": 1, "price": 89.99 }
  ],
  "total": 149.97
}
```

An entire order — customer info and line items included — lives as one document, retrievable in a single read with no joins, which is exactly the shape that tends to make document databases fast for read-heavy, aggregate-oriented access patterns (see Section 8 for when this is and isn't the right modeling choice).

---

## 3. MongoDB

### Basic CRUD

```javascript
db.products.insertOne({ name: "Wireless Mouse", price: 29.99, category: "electronics" });

db.products.find({ category: "electronics", price: { $lt: 50 } });

db.products.updateOne(
  { _id: ObjectId("...") },
  { $set: { price: 24.99 }, $push: { tags: "sale" } }
);

db.products.deleteOne({ _id: ObjectId("...") });
```

### The aggregation pipeline

MongoDB's **aggregation pipeline** is its primary tool for anything beyond simple filtering — a sequence of stages, each transforming the documents flowing through it, conceptually similar to a LINQ method chain or a SQL query broken into explicit steps:

```javascript
db.orders.aggregate([
  { $match: { status: "completed" } },
  { $unwind: "$items" },
  { $group: {
      _id: "$items.productId",
      totalRevenue: { $sum: { $multiply: ["$items.price", "$items.quantity"] } },
      unitsSold: { $sum: "$items.quantity" }
  }},
  { $sort: { totalRevenue: -1 } },
  { $limit: 10 }
]);
```

This computes top-10 products by revenue across all completed orders — `$match` filters, `$unwind` flattens the `items` array into one document per line item, `$group` aggregates by product, and `$sort`/`$limit` finish it off. The pipeline model makes complex, multi-step transformations explicit and composable, similar in spirit to a series of chained LINQ operators.

### Multi-document transactions

```javascript
const session = client.startSession();
session.startTransaction();
try {
  await accounts.updateOne({ _id: fromId }, { $inc: { balance: -100 } }, { session });
  await accounts.updateOne({ _id: toId }, { $inc: { balance: 100 } }, { session });
  await session.commitTransaction();
} catch (error) {
  await session.abortTransaction();
  throw error;
} finally {
  session.endSession();
}
```

MongoDB has supported full ACID multi-document transactions since version 4.0 (within a replica set) and across sharded clusters since 4.2 — a significant evolution from MongoDB's early reputation as offering only single-document atomicity, though multi-document transactions still carry a real performance cost relative to well-modeled single-document operations, and good data modeling (Section 8) that minimizes the need for them remains the preferred first choice where it fits the access pattern.

### Deployment options

- **Self-hosted** — run MongoDB yourself, on VMs, bare metal, or in containers/Kubernetes.
- **MongoDB Atlas** — MongoDB's own fully managed, multi-cloud DBaaS (available on Azure, AWS, and GCP), handling provisioning, scaling, backups, and patching.

---

## 4. Azure Cosmos DB

### A multi-model, globally distributed database service

Cosmos DB's defining architectural pitch is **global distribution with configurable consistency**, available transparently regardless of which API surface you use to talk to it.

```csharp
CosmosClient client = new CosmosClient(connectionString);
Container container = client.GetContainer("StoreDb", "Products");

var product = new Product { Id = "42", Name = "Wireless Mouse", Price = 29.99m, CategoryId = "electronics" };
await container.CreateItemAsync(product, new PartitionKey(product.CategoryId));

var query = container.GetItemQueryIterator<Product>(
    new QueryDefinition("SELECT * FROM c WHERE c.categoryId = @category")
        .WithParameter("@category", "electronics"));

while (query.HasMoreResults)
{
    foreach (var item in await query.ReadNextAsync())
        Console.WriteLine(item.Name);
}
```

### Multiple APIs, one underlying engine

| API | What it looks like to clients |
|---|---|
| **Core (SQL) API** | Native document model, queried with a SQL-like dialect (see below) |
| **API for MongoDB** | Speaks the MongoDB wire protocol — many MongoDB drivers/tools work against it with minimal changes |
| **API for Cassandra** | Speaks the Cassandra Query Language (CQL) wire protocol |
| **API for Gremlin** | Graph queries via the Apache TinkerPop Gremlin traversal language |
| **API for Table** | Compatible with Azure Table Storage's API, with more throughput/global distribution options |

This means an application already using MongoDB drivers can, in principle, point them at Cosmos DB's MongoDB API endpoint instead of a MongoDB server — though compatibility is close but not 100% identical to native MongoDB feature-for-feature, so this migration path needs real validation, not an assumption of a drop-in swap.

### SQL-like query language (Core API)

```sql
SELECT c.name, c.price
FROM c
WHERE c.categoryId = "electronics" AND c.price < 50
ORDER BY c.price DESC
```

Despite the SQL-like syntax, this queries a schema-less document container, not a relational table — `c` refers to each document ("item") in the container, and the query engine navigates nested properties directly (`c.address.city`, for instance) without needing a join.

### Global distribution

```csharp
// Configuring multi-region writes in Bicep/ARM, conceptually:
// locations: [ { locationName: "East US", failoverPriority: 0 }, { locationName: "West Europe", failoverPriority: 1 } ]
```

A Cosmos DB account can be replicated to any number of Azure regions worldwide with a few clicks/lines of IaC, with **multi-region writes** available so applications in different regions can write locally with low latency, and Cosmos DB handles propagating and (per the chosen consistency model, Section 6) reconciling those writes globally.

### Request Units (RUs): the throughput/cost currency

Cosmos DB doesn't bill by raw CPU/memory the way a VM-based database does — every operation (read, write, query) costs a measured number of **Request Units**, and you provision (or let autoscale handle) RU/s capacity that your workload's operations draw from:

```bash
az cosmosdb sql container create --account-name my-account --database-name StoreDb \
    --name Products --partition-key-path "/categoryId" --throughput 400
```

This RU-based cost model is genuinely distinctive versus most other databases — it's precise (you can see exactly how many RUs any given query consumed) but requires learning a new mental model for both performance tuning and cost estimation, since an inefficient query design (e.g., a cross-partition scan, Section 5) shows up directly as a higher RU cost, not just as "slow."

---

## 5. Partitioning and Sharding

### MongoDB: sharding

```javascript
sh.enableSharding("StoreDb");
sh.shardCollection("StoreDb.orders", { customerId: "hashed" });
```

MongoDB distributes a **sharded collection** across multiple shards based on a chosen **shard key** — a hashed shard key (as above) spreads writes evenly across shards, while a ranged shard key groups related values together (useful when range queries on the shard key are common), at the cost of potential "hot shard" issues if writes cluster around a narrow range of key values.

### Cosmos DB: partitioning

```bash
--partition-key-path "/categoryId"
```

Every Cosmos DB container requires a **partition key** chosen at creation time (and, in modern Cosmos DB, changeable later via a partition key migration, though it's still a meaningful decision to get right upfront) — it determines how documents are distributed across physical partitions, and it's the single most consequential data modeling decision in a Cosmos DB design.

### Choosing a good partition/shard key

A good key:
- **Distributes writes evenly** — avoid a key with a small number of very common values (e.g., a `status` field with only 3 possible values) which concentrates load on a few partitions/shards.
- **Matches your most common query filter** — queries that include the partition key can be routed directly to the relevant partition(s); queries that omit it become expensive **cross-partition (fan-out) queries** that touch every partition.
- **Has high enough cardinality** — a key like `customerId` (many distinct values) usually distributes better than something coarse like `region` (few distinct values, each potentially very large).

```sql
-- Efficient: includes the partition key, routed to a single partition
SELECT * FROM c WHERE c.categoryId = "electronics" AND c.price < 50

-- Expensive: no partition key filter, fans out across every partition
SELECT * FROM c WHERE c.price < 50
```

Getting the partition key wrong is one of the most common and most expensive-to-fix mistakes in both systems — it's usually a foundational, largely one-time decision (though both systems have added tooling to ease repartitioning after the fact), so it's worth real design effort upfront rather than an afterthought.

---

## 6. Consistency Models

### MongoDB: read concerns and write concerns

```javascript
db.orders.find().readConcern("majority");   // reads data acknowledged by a majority of replica set members
db.orders.insertOne(doc, { writeConcern: { w: "majority" } }); // waits for majority acknowledgment before returning
```

MongoDB lets you tune consistency/durability per-operation via **read concern** (how consistent/durable the data you're reading must be) and **write concern** (how many replicas must acknowledge a write before it's considered successful) — trading latency against consistency/durability guarantees on a per-query basis rather than a single fixed database-wide setting.

### Cosmos DB: five named consistency levels

Cosmos DB makes this tradeoff unusually explicit with five distinct, well-defined consistency levels, chosen at the account level (with the ability to override to a *weaker* level per-request):

| Level | Guarantee | Typical use |
|---|---|---|
| **Strong** | Linearizable — reads always see the latest committed write | Financial/critical data where staleness is unacceptable, at the cost of higher latency, especially across regions |
| **Bounded Staleness** | Reads lag writes by at most K versions or T time | Global apps needing a predictable, bounded staleness guarantee |
| **Session** (default) | A single client session always sees its own writes ("read your own writes") | The most common practical choice — good UX guarantee without the cost of global strong consistency |
| **Consistent Prefix** | Reads never see out-of-order writes, but may be stale | Workloads where seeing a consistent history matters more than recency |
| **Eventual** | No ordering guarantee at all, lowest latency | Workloads that can tolerate any staleness in exchange for maximum performance |

This spectrum — from the strictest, most expensive guarantee down to the loosest, cheapest one — is a direct, practical expression of the CAP theorem tradeoff that every distributed database has to make; Cosmos DB just makes the choice explicit and tunable rather than hiding it behind a single fixed default.

### The common thread

Both databases embrace the same underlying reality: in a globally distributed system, **strict consistency and low latency are in direct tension**, and rather than picking one tradeoff for you, both give you the tools to choose deliberately, per workload or even per query, based on what that specific data actually needs.

---

## 7. Indexing

### MongoDB

```javascript
db.products.createIndex({ categoryId: 1, price: -1 }); // compound index
db.products.createIndex({ name: "text", description: "text" }); // text search index
db.orders.createIndex({ location: "2dsphere" }); // geospatial index
```

MongoDB indexes work much like relational database indexes conceptually — compound indexes for common multi-field filters, plus specialized index types for text search and geospatial queries.

### Cosmos DB: automatic indexing by default

```json
{
  "indexingPolicy": {
    "automatic": true,
    "includedPaths": [{ "path": "/*" }],
    "excludedPaths": [{ "path": "/description/*" }]
  }
}
```

Cosmos DB's most distinctive indexing behavior: **every property of every document is indexed automatically by default**, with no need to declare indexes explicitly the way you would in MongoDB or a relational database. This is convenient (queries on arbitrary fields work efficiently out of the box) but has a real cost — indexing more data than you actually query on increases write RU cost and storage. **Excluding paths** you never query on (like a large, rarely-filtered `description` field above) is a common, worthwhile optimization once query patterns are well understood, rather than leaving the default "index everything" policy in place indefinitely for a mature, high-volume container.

---

## 8. Data Modeling: Embedding vs. Referencing

This is the central data modeling decision in both databases, and it's fundamentally different from relational normalization.

### Embedding: nest related data in one document

```json
{
  "orderId": "1001",
  "customer": { "name": "Ada Lovelace", "email": "ada@example.com" },
  "items": [ { "productId": "42", "quantity": 2 } ]
}
```

**Embed when:** the nested data is almost always read together with the parent, doesn't grow unboundedly (an order's line items are naturally bounded; a product's *all-time reviews* are not), and doesn't need to be queried/updated independently very often.

### Referencing: keep related data in a separate document, linked by ID

```json
// orders collection
{ "orderId": "1001", "customerId": "42", "items": [{ "productId": "42", "quantity": 2 }] }

// customers collection
{ "customerId": "42", "name": "Ada Lovelace", "email": "ada@example.com" }
```

**Reference when:** the related data is large, grows unboundedly (a customer's complete order history), is shared/reused across many parent documents (many orders reference the same customer), or needs to be updated independently without touching every document that references it.

### The core tradeoff

Embedding optimizes for **read performance** (fetch everything in one request) at the cost of some data duplication and update complexity (updating a customer's email means updating it in every embedded copy, if you'd embedded customer data into every order). Referencing optimizes for **update simplicity and bounded document size** at the cost of needing a second lookup (there's no server-side `JOIN` the way a relational database has one, though both systems offer limited lookup/join-like operations — MongoDB's `$lookup` aggregation stage, for instance).

There's no universally correct answer — the right choice depends entirely on the specific access pattern for that specific relationship, which is why document database schema design is often described as "designing around your queries" rather than designing around the data's inherent structure the way relational normalization does.

---

## 9. Querying

### MongoDB query syntax

```javascript
db.products.find(
  { category: "electronics", price: { $gte: 10, $lte: 100 } },
  { name: 1, price: 1, _id: 0 }
).sort({ price: -1 }).limit(20);
```

### Cosmos DB SQL syntax

```sql
SELECT c.name, c.price
FROM c
WHERE c.category = "electronics" AND c.price BETWEEN 10 AND 100
ORDER BY c.price DESC
OFFSET 0 LIMIT 20
```

Both support rich filtering, projection, sorting, and pagination — the syntax differs (MongoDB's query documents vs. Cosmos DB's SQL-like dialect), but the underlying capability and performance characteristics (efficient when the partition/shard key is included, expensive when it isn't) are conceptually similar.

### Change feeds: reacting to data changes

```csharp
// Cosmos DB change feed processor
var processor = container.GetChangeFeedProcessorBuilder<Product>("productChanges", HandleChangesAsync)
    .WithInstanceName("processor1")
    .WithLeaseContainer(leaseContainer)
    .Build();
await processor.StartAsync();
```

```javascript
// MongoDB change streams
const changeStream = db.collection('orders').watch();
changeStream.on('change', (change) => {
    console.log('Change detected:', change);
});
```

Both systems provide a way to subscribe to a real-time stream of document changes (inserts, updates, deletes) — useful for triggering downstream processing (updating a search index, invalidating a cache, sending a notification) without polling, conceptually similar to a database-level event stream feeding into the background-worker patterns covered elsewhere in this series.

---

## 10. .NET Integration

### Cosmos DB SDK

```csharp
builder.Services.AddSingleton(new CosmosClient(connectionString));

public class ProductRepository
{
    private readonly Container _container;
    public ProductRepository(CosmosClient client) =>
        _container = client.GetContainer("StoreDb", "Products");

    public async Task<Product?> GetByIdAsync(string id, string categoryId)
    {
        try
        {
            var response = await _container.ReadItemAsync<Product>(id, new PartitionKey(categoryId));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }
}
```

Note that a **point read** (`ReadItemAsync`, given both the `id` and partition key) is the cheapest, fastest possible operation against Cosmos DB — always prefer it over a query when you already know both values, rather than querying `WHERE c.id = @id` even though the result would be the same.

### MongoDB .NET Driver

```csharp
var client = new MongoClient(connectionString);
var database = client.GetDatabase("StoreDb");
var products = database.GetCollection<Product>("products");

var results = await products.Find(p => p.Category == "electronics" && p.Price < 50)
    .SortByDescending(p => p.Price)
    .Limit(20)
    .ToListAsync();
```

The official MongoDB .NET driver supports a strongly-typed LINQ-like query builder (as above) as well as raw BSON/JSON-style filter documents for more complex queries the typed builder doesn't express as cleanly.

### Entity Framework Core providers

Both ecosystems have EF Core providers (`Microsoft.EntityFrameworkCore.Cosmos` and MongoDB's own EF Core provider), letting you use familiar `DbContext`/LINQ patterns against a document database — though it's worth knowing this is a leakier abstraction than EF Core over a relational database: partition key design, RU cost awareness, and embedding/referencing decisions don't go away just because the API looks familiar, and teams that treat a document database exactly like a relational one through EF Core often end up fighting the model rather than benefiting from it.

---

## 11. Cosmos DB vs. MongoDB vs. a Relational Database

| Aspect | Azure Cosmos DB | MongoDB | Relational (SQL Server/PostgreSQL) |
|---|---|---|---|
| Schema | Flexible, per-document | Flexible, per-document | Fixed, enforced per-table |
| Scaling model | Horizontal by design, RU-based provisioning | Horizontal via sharding | Primarily vertical, with read replicas for read scaling |
| Global distribution | Native, first-class, multi-region writes | Possible (Atlas Global Clusters), less deeply native | Possible but more involved (geo-replication add-ons) |
| Consistency | Five explicit, tunable levels | Tunable via read/write concerns | Strong by default (ACID transactions) |
| Multi-document transactions | Supported within a partition natively; cross-partition more limited | Fully supported since 4.0/4.2 | Full, mature ACID transaction support — the traditional strength |
| Query language | SQL-like (Core API), or native MongoDB/Cassandra/Gremlin via other APIs | MongoDB Query Language + aggregation pipeline | SQL |
| Best for | Globally distributed apps, variable/evolving schemas, Azure-native architectures | Flexible-schema apps, teams wanting portability across clouds/on-prem | Complex relationships, strong consistency needs, heavy reporting/joins |

### When NoSQL document databases are the better fit

- Data is naturally document-shaped (a user profile, a product catalog with varied attributes, an order with nested line items) and rarely needs ad-hoc joins across unrelated entities.
- Schema needs to evolve frequently without formal migrations blocking every deploy.
- The application needs to scale horizontally to a degree a single relational server (even with read replicas) can't comfortably reach.
- Global, multi-region low-latency access is a core requirement (Cosmos DB in particular is purpose-built for this).

### When a relational database is still the better fit

- Data has many genuine relationships that need to be queried flexibly and joined in ways that aren't known upfront (ad-hoc reporting, complex multi-entity queries).
- Strong, straightforward multi-row/multi-table transactional guarantees are central to correctness (financial ledgers, inventory systems with strict consistency needs).
- The team's existing tooling, reporting stack, and expertise are deeply relational, and the workload doesn't have a compelling reason to diverge from that.

Many real systems use both — a relational database for core transactional data with well-understood relationships, and a document database for specific subsystems (product catalogs, user profiles, event/activity logs, session state) where flexible schema or massive read scale genuinely matters more than relational rigor.

---

## Quick Reference Table

| Concept | Cosmos DB | MongoDB |
|---|---|---|
| Unit of storage | Item, in a container | Document, in a collection |
| Partitioning key | Chosen at container creation, determines physical distribution | Shard key, chosen when sharding a collection |
| Query language | SQL-like (Core API); also Mongo/Cassandra/Gremlin via other APIs | MongoDB Query Language + aggregation pipeline |
| Consistency | 5 explicit levels: Strong → Bounded Staleness → Session → Consistent Prefix → Eventual | Tunable read/write concerns per operation |
| Billing model | Request Units (RU/s), provisioned or autoscale | Compute/storage-based (self-hosted) or Atlas tier-based |
| Default indexing | Every property indexed automatically | Explicit index creation required |
| Managed offering | Native Azure service | MongoDB Atlas (multi-cloud) |
| Change notifications | Change Feed | Change Streams |

---

## Conclusion

Cosmos DB and MongoDB both embrace the same core document-database philosophy — flexible schemas, horizontal scale by design, and modeling around access patterns rather than rigid normalization — but express it differently. Cosmos DB leans into globally distributed, multi-region, tunable-consistency architecture as a first-class, built-in concern with an unusually explicit RU-based cost model; MongoDB leans into a mature, widely adopted query and aggregation model with strong multi-cloud portability via Atlas.

Choosing between them (and between either and a relational database) comes down to the same question that recurs throughout this series: what does the actual access pattern and consistency requirement of this specific workload need, not which technology is generically "better." Document databases earn their complexity when data is naturally document-shaped and scale/schema-flexibility genuinely matter; a well-modeled relational database remains the right, simpler choice for a great deal of application data that doesn't actually need either of those things.

---

*Found this useful? Feel free to star the repo, open an issue with corrections, or share the partition key decision you wish you'd gotten right the first time.*
