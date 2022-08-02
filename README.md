# Query autocompleetion demo

Cassandra based implementation of [prefixy](https://prefixy.github.io/).
[Medium post](https://medium.com/@prefixyteam/how-we-built-prefixy-a-scalable-prefix-search-service-for-powering-autocomplete-c20f98e2eff1)
of the same document.

## Main requirements

- Get completions p90 latency <= 100ms
- 1000 queries per second, QPS, can be handled by single host deployment
- Unicode charset support

## Run demo

To run demo on local machine [docker](https://www.docker.com/)
must be installed and running. Internet connection is also required
to download all the dependencies and docker images.
Note that the docker compose file
exposes ports 8080 and 3000 to the local host, so these ports must be available.

```bash
docker compose up
```

eventually starts one webserver instance, one cassandra instance, demo GUI
and starts queries "populator" that adds 1 million of real queries to
the database. It might take some time for all services to be up.

This log message of **query-autocompleetion-webserver-1** container indicates that webserver is up and running:

```bash
2022-08-02 14:52:35,799 [main] INFO boot.StartupInfoLogger:61 - Started Application in 7.621 seconds (JVM running for 9.336)
```

The log messages of **query-autocompleetion-query-populator-1** container
that show query populator progress are:

```bash
2022-08-02 15:14:31,835 [ForkJoinPool-11] INFO bufistov.QueryPopulator:134 - 970000 queries done...
```

```aidl
curl http://localhost:8080/queries?prefix=google
```

should work.

Once all services are up and running open this link in browser:
http://localhost:3000

type queries in the text box. On enter press query is actually
added into database. Notice that the query must be added at least 5 times (press enter 5 times)
for the first topK rank update. Later updates for the same query
will happen every 10 new occurrence or every 5 seconds.

## Stop demo

```bash
docker compose down
```
