package org.bufistov;

public class Constants {
    /**
     * Cassandra keyspace for the tables.
     */
    public static final String CASSANDRA_KEYSPACE = "autocompletedemo";

    /**
     * Table to store query counters.
     */
    public static final String QUERY_COUNT = "query_count";

    /**
     * Table to store last timestamp of update topk for given query.
     */
    public static final String QUERY_UPDATE = "query_update";


    /**
     * Table to store trie-heap structure of topK prefixes for each query.
     */
    public static final String PREFIX_TOPK = "prefix_topk";

    public static final String SUFFIX_COUNT_TYPE = "SuffixCount";
}
