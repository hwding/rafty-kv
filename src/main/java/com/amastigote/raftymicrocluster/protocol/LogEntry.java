package com.amastigote.raftymicrocluster.protocol;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author: hwding
 * @date: 2018/12/9
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@ToString
public class LogEntry {
    private int term;
    private Object data;
}
