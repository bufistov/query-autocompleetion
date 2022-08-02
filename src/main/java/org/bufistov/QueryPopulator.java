package org.bufistov;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

@Log4j2
public class QueryPopulator {
    static final String DEFAULT_QUERY_FILE = "aol_2006_100K_queries.txt.gz";
    static final int DEFAULT_NUM_THREADS = 16;

    static final String DEFAULT_PREFIX_TO_COUNT = "www";

    static HttpClient httpClient = getHttpClient();

    static AtomicLong failuresCounter = new AtomicLong();

    static AtomicLong queriesCounter = new AtomicLong();

    public static void main(String[] args) {
        String queryFile = DEFAULT_QUERY_FILE;
        if (args.length > 0) {
            queryFile = args[0];
        }
        log.info("Reading query file: {}", queryFile);
        int numThreads = DEFAULT_NUM_THREADS;
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        log.info("Num threads: {}", numThreads);
        configureGlobalThreadPool(numThreads);

        final String prefixToCount = args.length > 2 ? args[2] : DEFAULT_PREFIX_TO_COUNT;
        log.info("Prefix to count: {}", prefixToCount);

        final boolean populateQueries = args.length <= 3;

        List<String> queries = readQueries(queryFile);

        log.info("Read {} queries", queries.size());
        Map<String, Long> counted = queries.parallelStream().collect(
                Collectors.groupingBy(Function.identity(),Collectors.counting()));
        log.info("{} unique queries", counted.size());
        var mostFrequent = Collections.max(counted.entrySet(),
                (x,  y) -> (int) (x.getValue() - y.getValue()));
        log.info("Most frequent query: '{}' count: {}", mostFrequent.getKey(), mostFrequent.getValue());
        long withGivenPrefix = counted.entrySet().parallelStream()
                .filter(x -> x.getKey().startsWith(prefixToCount))
                .map(Map.Entry::getValue)
                .reduce(0L, Long::sum);
        log.info("Queries with prefix '{}' {}", prefixToCount, withGivenPrefix);
        log.info("Max query length: {}", queries.stream().map(String::length).max(Integer::compareTo).get());
        log.info("Average query length: {}", queries.stream().map(String::length).reduce(0, Integer::sum) / queries.size());
        log.info("Queries > 100: {}", queries.stream().filter(q -> q.length() > 100).count());
        log.info("Queries > 50: {}", queries.stream().filter(q -> q.length() > 50).count());
        log.info("Queries > 20: {}", queries.stream().filter(q -> q.length() > 20).count());
        log.info("Queries > 10: {}", queries.stream().filter(q -> q.length() > 10).count());
        if (populateQueries) {
            long start = System.currentTimeMillis();

            var latencies = queries.parallelStream()
                    .map(QueryPopulator::addQuery)
                    .collect(Collectors.toList());
            long totalTimeSeconds = Math.max(1, (System.currentTimeMillis() - start) / 1000);
            log.info("{} queries in {} seconds", queries.size(), totalTimeSeconds);
            log.info("Error rate: {}", failuresCounter.get() / (double) queries.size());
            log.info("QPS {}", queries.size() / totalTimeSeconds);
            log.info("Min latency: {}", Collections.min(latencies));
            log.info("Max latency: {}", Collections.max(latencies));
            log.info("Avg latency: {}", latencies.parallelStream().reduce(0L, Long::sum) / latencies.size());
            Collections.sort(latencies);
            log.info("P90 latency: {}", percentile(latencies, 90));
            log.info("P99 latency: {}", percentile(latencies, 99));
        }
    }

    static HttpClient getHttpClient() {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .proxy(ProxySelector.of(null))
                .build();
    }

    static void configureGlobalThreadPool(int size) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", Integer.toString(size));
    }

    @SneakyThrows
    static long addQuery(String query) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:8080/add_query", getBackend())))
                .timeout(Duration.ofSeconds(20))
                .header("content-type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(query, StandardCharsets.UTF_8))
                .build();
        long start = System.currentTimeMillis();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        long finish = System.currentTimeMillis();
        if (response.statusCode() / 100 != 2) {
            log.error(response.body());
            failuresCounter.incrementAndGet();
        }
        var current = queriesCounter.incrementAndGet();
        if (current % 10000 == 0) {
            log.info("{} queries done...", current);
        }
        return finish - start;
    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    static List<String> readQueries(String fileName) {
        if (fileName.endsWith(".gz")) {
            return readQueriesCompressed(fileName);
        } else {
            return readQueriesNotCompressed(fileName);
        }
    }

    static List<String> readQueriesNotCompressed(String fileName) {
        try (var inputFileStream = new FileInputStream(fileName)) {
            return getLines(inputFileStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static List<String> readQueriesCompressed(String fileName) {
        try (var inputFileStream = new GZIPInputStream(new FileInputStream(fileName))) {
            return getLines(inputFileStream);
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    static List<String> getLines(InputStream inputStream) {
        try(Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            ArrayList<String> lines = new ArrayList<>();
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
            return lines;
        }
    }

    static String getBackend() {
        return System.getenv().getOrDefault("BACKEND_HOST", "localhost");
    }
}
