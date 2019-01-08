package com.amastigote.raftymicrocluster.disk;

import com.amastigote.raftymicrocluster.NodeState;
import com.amastigote.raftymicrocluster.conf.NodeGlobalConf;
import com.amastigote.raftymicrocluster.protocol.LogEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static com.amastigote.raftymicrocluster.conf.NodeGlobalConf.KEY_CHECKPOINT_CACHE_SIZE;
import static com.amastigote.raftymicrocluster.conf.NodeGlobalConf.KEY_PERSIST_DIR;

/**
 * @author: hwding
 * @date: 2019/1/2
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "PERSIST")
public final class PersistManager {
    private static AppendableStateSerializer stateSerializer;
    private static PersistManager instance = new PersistManager();
    private boolean recoverable = true;

    private PersistManager() {
        try {
            final String persistDir = NodeGlobalConf.readConf(KEY_PERSIST_DIR);
            final int checkpointCacheSize = Integer.valueOf(NodeGlobalConf.readConf(KEY_CHECKPOINT_CACHE_SIZE));

            File file = new File(persistDir, String.valueOf(NodeState.nodePort()));
            if (!file.exists()) {
                boolean created = file.createNewFile();

                if (!created) {
                    throw new IOException("persist file failed to create");
                }

                log.info("persist file newly created");
                recoverable = false;
            }

            RandomAccessFile persistFile = new RandomAccessFile(file, "rws");

            stateSerializer = new AppendableStateSerializer(persistFile, checkpointCacheSize);

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

        NodeState.updateTerm(term);
        NodeState.voteFor(vote);
        NodeState.appendEntryUnaltered(entries);

        log.info("recovered with term {}, votedFor {}, entries {}", term, vote, entries.size());
        return true;
    }

    public void persistCurrentTerm(int term) {
        try {
            stateSerializer.persistCurTerm(term);
        } catch (IOException e) {
            log.error("failed to persist curTerm {}", term, e);
        }
    }

    public void persistVotedFor(int votedFor) {
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
