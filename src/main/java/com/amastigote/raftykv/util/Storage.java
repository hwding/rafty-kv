package com.amastigote.raftykv.util;

import com.amastigote.raftykv.protocol.LogEntry;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * @author: hwding
 * @date: 2018/12/20
 */
@SuppressWarnings("JavaDoc")
public final class Storage {
    private Map<Object, TermTaggedValue> storageMap = new ConcurrentHashMap<>();
    private Consumer<LogEntry> commandExecutor = logEntry -> {
        final LogEntry.LogCommandType type = logEntry.getLogCommandType();

        switch (type) {
            case PUT:
                put(logEntry);
                break;
            case REMOVE:
                remove(logEntry.getKey());
                break;
        }
    };

    private void put(LogEntry entry) {
        storageMap.put(entry.getKey(), entry);
    }

    public Object get(Object key) {
        return storageMap.get(key).getV();
    }

    private void remove(Object key) {
        storageMap.remove(key);
    }

    /* call from com.amastigote.raftykv.NodeState only */
    public void applyEntryCommands(List<LogEntry> logEntries) {
        logEntries.parallelStream().forEachOrdered(commandExecutor);
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class TermTaggedValue {
        private int t;
        private Object v;

        public TermTaggedValue(int term, Object value) {
            this.t = term;
            this.v = value;
        }

        public TermTaggedValue(Object value) {
            this.v = value;
        }
    }
}
