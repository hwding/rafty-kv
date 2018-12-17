package com.amastigote.raftymicrocluster.protocol;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Getter
@Setter
@ToString
public class GeneralMsg implements Serializable {
    private MsgType msgType;
    private MsgType.ElectMsgType electMsgType;
    private int term;

    /* servers as leaderId in this case */
    private int responseToPort;

    /* >> AppendEntryMsg only */
    private int prevLogIdx;
    private int prevLogTerm;
    private LogEntry[] entries;
    private int committedIdx;
    /* << AppendEntryMsg only */

    /* >> RequestVoteMsg only */
    private int lastLogIdx;
    private int lastLogTerm;
    /* << RequestVoteMsg only */
}
