package com.amastigote.raftymicrocluster.protocol;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: hwding
 * @date: 2018/12/9
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LogEntry implements Map.Entry, Serializable {
    private int term;
    private Object key, value;
    private LogCommandType logCommandType;

    public LogEntry(Object key, Object value, LogCommandType logCommandType) {
        this.key = key;
        this.value = value;
        this.logCommandType = logCommandType;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object setValue(Object value) {
        this.value = value;
        return this.value;
    }

    public enum LogCommandType {
        PUT,
        REMOVE
    }
}
