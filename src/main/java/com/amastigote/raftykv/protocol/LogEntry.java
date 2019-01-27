package com.amastigote.raftykv.protocol;

import com.amastigote.raftykv.util.Storage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: hwding
 * @date: 2018/12/9
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@ToString
@NoArgsConstructor
public class LogEntry extends Storage.TermTaggedValue implements Map.Entry, Serializable {
    private Object key;
    private LogCommandType logCommandType;

    public LogEntry(Object key, Object value, LogCommandType logCommandType) {
        super(value);

        this.key = key;
        this.logCommandType = logCommandType;
    }

    public LogEntry(int term, Object key, Object value, LogCommandType logCommandType) {
        super(term, value);

        this.key = key;
        this.logCommandType = logCommandType;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return super.getT();
    }

    @Override
    public Object setValue(Object value) {
        super.setV(value);
        return super.getV();
    }

    public int getTerm() {
        return super.getT();
    }

    public void setTerm(int t) {
        super.setT(t);
    }

    public enum LogCommandType {
        PUT,
        REMOVE
    }
}
