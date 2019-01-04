package com.amastigote.raftymicrocluster.disk;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Properties;

/**
 * @author: hwding
 * @date: 2019/1/2
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "PERSIST")
public final class PersistManager {
    private final static String CONFIGURATION_PREFIX = "com.amastigote.raftymicrocluster.";
    private final static String CONFIGURATION_FILE = "rafty-persist.properties";
    private final static String KEY_END_PERSIST_DIR = "persistDir";
    private final static String KEY_PERSIST_DIR = CONFIGURATION_PREFIX + KEY_END_PERSIST_DIR;
    private final static String DEFAULT_VAL_PERSIST_DIR = ".";
    private static AppendableStateSerializer stateSerializer;
    private final Properties properties = new Properties();
    private boolean recoverable = true;

    private static PersistManager instance = new PersistManager();

    private PersistManager() {
        try {
            loadProperties();

            String persistDir = properties.getProperty(KEY_PERSIST_DIR, DEFAULT_VAL_PERSIST_DIR);

            File file = new File(persistDir, String.valueOf(NodeStatus.nodePort()));
            if (!file.exists()) {
                boolean created = file.createNewFile();

                if (!created) {
                    throw new IOException("persist file failed to create");
                }

                log.info("persist file newly created");
                recoverable = false;
            }

            RandomAccessFile persistFile = new RandomAccessFile(file, "rws");

            stateSerializer = new AppendableStateSerializer(persistFile);

            if (!recoverable) {
                stateSerializer.initFile();
            }
        } catch (IOException e) {
            log.error("persist file failed to init", e);
            System.exit(-1);
        }
    }

    public static PersistManager getInstance() {
        return instance;
    }

    private void loadProperties() {
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

    public boolean recover() {
        if (!recoverable) {
            return false;
        }

        int term, vote;
        List<LogEntry> entries;
        try {
            term = stateSerializer.recoverCurTerm();
            vote = stateSerializer.recoverVotedFor();
            entries = stateSerializer.recoverEntries();
        } catch (Exception e) {
            log.error("error during recover, give up", e);
            return false;
        }

        NodeStatus.updateTerm(term);
        NodeStatus.voteFor(vote);
        NodeStatus.appendEntryUnaltered(entries);

        log.info("recovered with term {}, votedFor {}, entries {}", term, vote, entries.size());
        return true;
    }

    void persistCurrentTerm(int term) {
        try {
            stateSerializer.persistCurTerm(term);
        } catch (IOException e) {
            log.error("failed to persist curTerm {}", term, e);
        }
    }

    void persistVotedFor(int votedFor) {
        try {
            stateSerializer.persistVotedFor(votedFor);
        } catch (IOException e) {
            log.error("failed to persist votedFor {}", votedFor, e);
        }
    }

    public void persistLogEntry(LogEntry entry) {
        try {
            stateSerializer.persistLogEntry(entry);
        } catch (IOException e) {
            log.error("failed to persist log entry {}", entry, e);
        }
    }

    public void truncateLogEntry(int toIdxExclusive) {
        try {
            stateSerializer.truncateLogEntry(toIdxExclusive);
        } catch (Exception e) {
            log.warn("failed to truncate log entry to idx exclusive {}", toIdxExclusive, e);
        }
    }
}
