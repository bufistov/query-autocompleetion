# Query complete demo

This repository contains backend implementation for query
complete suggestion using 10 previous most frequent queries with the same
prefix. A simple GUI is also provided to run demo interactively.
Demo is started by executing one command in OS command shell: **docker compose up**

![Demo logo](images/query_complete_demo_logo.png)

This is mainly a cassandra based implementation of [prefixy](https://prefixy.github.io/).
[Medium post](https://medium.com/@prefixyteam/how-we-built-prefixy-a-scalable-prefix-search-service-for-powering-autocomplete-c20f98e2eff1)
of the same document.

## Product Requirements

- Get completions p90 latency <= 100ms.
- 1000 queries per second, QPS, can be handled on my mac with 16GB of random access memory.
- Solve problem exactly, provide actual top10 most frequent queries with given prefix.
- Scale horizontally.
- Support unicode charset.

## Run demo locally

To run demo on local machine [docker](https://www.docker.com/)
must be installed and running. Internet connection is also required
to download all the dependencies and docker images.

Notes:

- The docker compose file
exposes ports 8080 and 3000 to the local host, so these ports must be available.
- The demo adds one million queries, and will use most of the CPU/disk
bandwith of the host machine during first 10-20 minutes.

Start demo by typing this command from the root directory of this repository:

```bash
docker compose up
```

This should eventually start one webserver instance, one cassandra instance, demo GUI
and starts queries "populator" that adds 1 million of real queries to
the database. It might take several minutes for all services to be up.

The logs of the webserver are stored in logs/ directory of the repository root.
Cassandra data are mapped into datadc/ directory of the repository root.

This log message of **query-autocompleetion-webserver-1** container indicates that webserver is up and running:

```bash
2022-08-02 14:52:35,799 [main] INFO boot.StartupInfoLogger:61 - Started Application in 7.621 seconds (JVM running for 9.336)
```

The log messages of **query-autocompleetion-query-populator-1** container
that show query populator progress are:

```bash
2022-08-02 15:14:31,835 [ForkJoinPool-11] INFO bufistov.QueryPopulator:134 - 970000 queries done...
```

The command
```aidl
curl http://localhost:8080/queries?prefix=google
```

should work on local machine.

Once all services are up and running open this link in browser:
http://localhost:3000

type queries in the text box. On 'enter' key press query is actually
added into database. Notice, that the query must be added at least 3 times (press enter 3 times)
for the first topK rank update. Later updates for the same query
happen every 10 new occurrence or every 5 seconds.

## Destroy demo

```bash
docker compose down
```

# Development setup

Docker (latest), java (>= 11) and node (>= 15) are required to run
webserver and frontend on local machine.

## Things to improve
- Test horizontal scalability
- Counters does not accept time to leave eviction in Cassandra.
- Upgrade to cassandra java driver 4.0
- Add observability metrics
- Improve integration tests by using spring test runner and avoid
manual creation of all beans
- Unit test frontend
