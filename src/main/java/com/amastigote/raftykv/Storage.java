package com.amastigote.raftykv;

import com.amastigote.raftykv.protocol.LogEntry;

import java.util.Collection;
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
    private Map<Object, Object> storageMap = new ConcurrentHashMap<>();
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

    private void putAll(Collection<Map.Entry> entries) {
        entries.parallelStream().forEach(e -> storageMap.put(e.getKey(), e.getValue()));
    }

    private void put(Map.Entry entry) {
        storageMap.put(entry.getKey(), entry.getValue());
    }

    public Object get(Object key) {
        return storageMap.get(key);
    }

    private void remove(Object key) {
        storageMap.remove(key);
    }

    void applyEntryCommands(List<LogEntry> logEntries) {
        logEntries.parallelStream().forEachOrdered(commandExecutor);
    }
}
