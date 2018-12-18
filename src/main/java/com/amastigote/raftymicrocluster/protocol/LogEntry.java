package com.amastigote.raftymicrocluster.protocol;

import lombok.*;

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
public class LogEntry implements Map.Entry {
    private int term;
    private Object key, value;

    public LogEntry(Object key, Object value) {
        this.key = key;
        this.value = value;
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
}
