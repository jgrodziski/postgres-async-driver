# postgres-async-driver - Asynchronous PostgreSQL Java driver

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jaceksokol/postgres-async-driver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.jaceksokol/postgres-async-driver/)

Postgres-async-driver is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions, timeouts, back-pressure all standard SQL types and custom column types. 

**This is a fork of [Antti Laisi's](https://github.com/alaisi/postgres-async-driver) PostgreSQL asynchronous driver.** Some refactorings has been applied in order to implement back-pressure (currently only on TCP level) and timeouts.

## Download

Postgres-async-driver is available on [Maven Central](http://search.maven.org/#search|ga|1|g%3A%22com.github.jaceksokol).

```xml
<dependency>
    <groupId>com.github.jaceksokol</groupId>
    <artifactId>postgres-async-driver</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

### Creating a Db

Db is a connection pool that is created with [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/jaceksokol/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
Db db = new ConnectionPoolBuilder()
    .hostname("localhost")
    .port(5432)
    .database("db")
    .username("user")
    .password("pass")
    .poolSize(20)
    .connectTimeout(1, TimeUnit.SECONDS)
    .statementTimeout(10, TimeUnit.SECONDS)
    .build();
```

Each connection *pool* will start only one IO thread used in communicating with PostgreSQL backend and executing callbacks for all connections.

### Hello world

Querying for a set returns an [rx.Observable](http://reactivex.io/documentation/observable.html) that emits a single [ResultSet](https://github.com/jaceksokol/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ResultSet.java).
This method does not supports back-pressure.

```java
db.querySet("select 'Hello world!' as message")
    .map(result -> result.row(0).getString("message"))
    .subscribe(System.out::println)

// => Hello world
```

Querying for rows returns an [rx.Observable](http://reactivex.io/documentation/observable.html) that emits 0-n [Rows](https://github.com/jaceksokol/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/Row.java). The rows are emitted immediately as they are received from the server instead of waiting for the entire query to complete.
This method supports back-pressure.

```java
db.queryRows("select unnest('{ hello, world }'::text[] as message)")
    .map(row -> row.getString("message"))
    .subscribe(System.out::println)

// => hello
// => world
```

### Prepared statements

Prepared statements use native PostgreSQL syntax `$index`. Supported parameter types are all primitive types, `String`, `BigDecimal`, `BigInteger`, `UUID`, temporal types in `java.sql` package and `byte[]`.

```java
db.querySet("insert into message(id, body) values($1, $2)", 123, "hello")
  .subscribe(result -> out.printf("Inserted %d rows", result.updatedRows()));
```

### Transactions

A transactional unit of work is started with `begin()`. Queries issued to the emitted [Transaction](https://github.com/jaceksokol/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/Transaction.java) are executed in the same transaction and the tx is automatically rolled back on query failure.

```java
db.begin()
    .flatMap(tx -> tx.querySet("insert into products (name) values ($1) returning id", "saw")
        .map(productsResult -> productsResult.row(0).getLong("id"))
        .flatMap(id -> tx.querySet("insert into promotions (product_id) values ($1)", id))
        .flatMap(promotionsResult -> tx.commit())
    ).subscribe(
        __ -> System.out.println("Transaction committed"),
        Throwable::printStackTrace
    );
```

### Timeouts

You can set default statement timeout for `ConnectionPool` and additionally per query.

```java
db.withTimeout(1, TimeUnit.SECONDS).queryRows("select * from events").subscribe();
```

### Custom data types

Support for additional data types requires registering converters to [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/jaceksokol/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
class JsonConverter implements Converter<example.Json> {
    @Override
    public Class<example.Json> type() {
        return example.Json.class;
    }
    @Override
    public byte[] from(example.Json json) {
        return json.toBytes();
    }
    @Override
    public example.Json to(Oid oid, byte[] value) {
        return new example.Json(new String(value, UTF_8));
    }
}

Db db = new ConnectionPoolBuilder()
//    ...
    .converters(new JsonConverter())
    .build();
```

## Used in

## References
* [Antti Laisi's postgres-async-driver](https://github.com/alaisi/postgres-async-driver)
* [Scala postgresql-async](https://github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org/about/about.html)

