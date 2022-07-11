package org.bufistov.autocomplete;

import com.datastax.driver.core.Cluster;
import lombok.extern.log4j.Log4j2;
import org.bufistov.SpringConfiguration;
import org.bufistov.handler.QueryComplete;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Log4j2
public class QueryCompleteIntegTest {

    final static Long TOPK = 10L;
    final static Long MAX_RETRIES_TO_UPDATE_TOPK = 10L;

    SpringConfiguration springConfiguration = new SpringConfiguration();
    QueryComplete queryComplete;

    String queryPrefix;

    Random random = new Random(0);
    private int maxThreadPoolSize = 1000;

    Cluster provideCluster(int cassandraPort) {
        return Cluster.builder()
                .addContactPointsWithPorts(InetSocketAddress.createUnresolved("localhost", cassandraPort))
                .withoutJMXReporting()
                .build();
    }

    QueryComplete provideQueryComplete() {
        var storage = springConfiguration.provideStorage(provideCluster(9042));
        var queryHandler = new QueryHandlerImpl(storage, TOPK, MAX_RETRIES_TO_UPDATE_TOPK,
                new UniformRandomInterval(random),
                suffixUpdateExecutorService(), null);
        return new QueryComplete(queryHandler);
    }

    RandomInterval provideRandomInterval() {
        return new UniformRandomInterval(new Random(0));
    }

    public ExecutorService suffixUpdateExecutorService() {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(cpuNum, maxThreadPoolSize, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    }

    @BeforeEach
    void setUp() {
        queryComplete = provideQueryComplete();
        queryPrefix = Integer.toHexString(random.nextInt());
        if (queryPrefix.length() > 4) {
            queryPrefix = queryPrefix.substring(0, 4);
        }
    }

    @Test
    void test1() {
        log.info("Query prefix: {}", queryPrefix);
        var res = queryComplete.queries("Wh");
        System.err.println(res.toString());
    }
}
