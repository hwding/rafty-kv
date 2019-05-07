package com.amastigote.raftykv.conf;

import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: hwding
 * @date: 2019/1/8
 */
@SuppressWarnings("JavaDoc")
public final class NodeGlobalConf {
    private final static Properties properties = new Properties();

    private final static String CONFIGURATION_FILE = "rafty-node.properties";
    private final static String CONFIGURATION_PREFIX = "com.amastigote.rafty-kv.";

    private final static String __KEY_PERSIST_DIR = "persistDir";
    public final static String KEY_PERSIST_DIR = CONFIGURATION_PREFIX + __KEY_PERSIST_DIR;
    private final static String __KEY_CHECKPOINT_CACHE_SIZE = "checkPointCacheSize";
    public final static String KEY_CHECKPOINT_CACHE_SIZE = CONFIGURATION_PREFIX + __KEY_CHECKPOINT_CACHE_SIZE;
    private final static String __KEY_BIND_RAND_PORT_MAX_RETRY = "bindRandPortMaxRetry";
    public final static String KEY_BIND_RAND_PORT_MAX_RETRY = CONFIGURATION_PREFIX + __KEY_BIND_RAND_PORT_MAX_RETRY;
    private final static String __KEY_RAND_PORT_MIN = "randPortMin";
    public final static String KEY_RAND_PORT_MIN = CONFIGURATION_PREFIX + __KEY_RAND_PORT_MIN;
    private final static String __KEY_RAND_PORT_MAX = "randPortMax";
    public final static String KEY_RAND_PORT_MAX = CONFIGURATION_PREFIX + __KEY_RAND_PORT_MAX;
    private final static String __KEY_HEARTBEAT_TIMEOUT_MIN_MILLIS = "heartbeatTimeoutMinMillis";
    public final static String KEY_HEARTBEAT_TIMEOUT_MIN_MILLIS = CONFIGURATION_PREFIX + __KEY_HEARTBEAT_TIMEOUT_MIN_MILLIS;
    private final static String __KEY_HEARTBEAT_TIMEOUT_MAX_MILLIS = "heartbeatTimeoutMaxMillis";
    public final static String KEY_HEARTBEAT_TIMEOUT_MAX_MILLIS = CONFIGURATION_PREFIX + __KEY_HEARTBEAT_TIMEOUT_MAX_MILLIS;
    private final static String __KEY_ELECTION_TIMEOUT_MIN_MILLIS = "electionTimeoutMinMillis";
    public final static String KEY_ELECTION_TIMEOUT_MIN_MILLIS = CONFIGURATION_PREFIX + __KEY_ELECTION_TIMEOUT_MIN_MILLIS;
    private final static String __KEY_ELECTION_TIMEOUT_MAX_MILLIS = "electionTimeoutMaxMillis";
    public final static String KEY_ELECTION_TIMEOUT_MAX_MILLIS = CONFIGURATION_PREFIX + __KEY_ELECTION_TIMEOUT_MAX_MILLIS;
    private final static String __KEY_HEARTBEAT_INTERVAL_MILLIS = "heartbeatIntervalMillis";
    public final static String KEY_HEARTBEAT_INTERVAL_MILLIS = CONFIGURATION_PREFIX + __KEY_HEARTBEAT_INTERVAL_MILLIS;
    private final static String __KEY_REPLICATE_SLICE_ENTRY_SIZE = "replicateSliceEntrySize";
    public final static String KEY_REPLICATE_SLICE_ENTRY_SIZE = CONFIGURATION_PREFIX + __KEY_REPLICATE_SLICE_ENTRY_SIZE;

    private static final String DEFAULT_VAL_PERSIST_DIR = ".";
    private static final String DEFAULT_VAL_CHECKPOINT_CACHE_SIZE = "20";
    private static final String DEFAULT_VAL_BIND_RAND_PORT_MAX_RETRY = "3";
    private static final String DEFAULT_VAL_RAND_PORT_MIN = "18000";
    private static final String DEFAULT_VAL_RAND_PORT_MAX = "18099";
    private final static String DEFAULT_VAL_HEARTBEAT_TIMEOUT_MIN_MILLIS = "4000";
    private final static String DEFAULT_VAL_HEARTBEAT_TIMEOUT_MAX_MILLIS = "5500";
    private final static String DEFAULT_VAL_ELECTION_TIMEOUT_MIN_MILLIS = "2500";
    private final static String DEFAULT_VAL_ELECTION_TIMEOUT_MAX_MILLIS = "4000";
    private final static String DEFAULT_VAL_HEARTBEAT_INTERVAL_MILLIS = "1500";
    private final static String DEFAULT_VAL_REPLICATE_SLICE_ENTRY_SIZE = "5";

    private static final Map<String, String> defaultValMap;

    static {
        defaultValMap = new HashMap<>();

        defaultValMap.put(KEY_PERSIST_DIR, DEFAULT_VAL_PERSIST_DIR);
        defaultValMap.put(KEY_CHECKPOINT_CACHE_SIZE, DEFAULT_VAL_CHECKPOINT_CACHE_SIZE);
        defaultValMap.put(KEY_BIND_RAND_PORT_MAX_RETRY, DEFAULT_VAL_BIND_RAND_PORT_MAX_RETRY);
        defaultValMap.put(KEY_RAND_PORT_MIN, DEFAULT_VAL_RAND_PORT_MIN);
        defaultValMap.put(KEY_RAND_PORT_MAX, DEFAULT_VAL_RAND_PORT_MAX);
        defaultValMap.put(KEY_HEARTBEAT_TIMEOUT_MIN_MILLIS, DEFAULT_VAL_HEARTBEAT_TIMEOUT_MIN_MILLIS);
        defaultValMap.put(KEY_HEARTBEAT_TIMEOUT_MAX_MILLIS, DEFAULT_VAL_HEARTBEAT_TIMEOUT_MAX_MILLIS);
        defaultValMap.put(KEY_ELECTION_TIMEOUT_MIN_MILLIS, DEFAULT_VAL_ELECTION_TIMEOUT_MIN_MILLIS);
        defaultValMap.put(KEY_ELECTION_TIMEOUT_MAX_MILLIS, DEFAULT_VAL_ELECTION_TIMEOUT_MAX_MILLIS);
        defaultValMap.put(KEY_HEARTBEAT_INTERVAL_MILLIS, DEFAULT_VAL_HEARTBEAT_INTERVAL_MILLIS);
        defaultValMap.put(KEY_REPLICATE_SLICE_ENTRY_SIZE, DEFAULT_VAL_REPLICATE_SLICE_ENTRY_SIZE);
    }

    private NodeGlobalConf() {
    }

    public synchronized static void init() {
        loadProperties();
    }

    private static void loadProperties() {
        InputStream in = AccessController.doPrivileged((PrivilegedAction<InputStream>) () -> {
            ClassLoader threadCL = Thread.currentThread().getContextClassLoader();
            if (threadCL != null) {
                return threadCL.getResourceAsStream(CONFIGURATION_FILE);
            } else {
                return ClassLoader.getSystemResourceAsStream(CONFIGURATION_FILE);
            }
        });
        if (null != in) {
            try {
                properties.load(in);
            } catch (java.io.IOException ignored) {
            } finally {
                try {
                    in.close();
                } catch (java.io.IOException ignored) {
                }
            }
        }
    }

    public static String readConf(final String key) throws IllegalArgumentException {
        if (!defaultValMap.keySet().contains(key)) {
            throw new IllegalArgumentException("no such conf key: " + key);
        }

        return properties.getProperty(key, defaultValMap.get(key));
    }
}
