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
    private Integer term;

    /* servers as leaderId in this case */
    private Integer responseToPort;

    /* >> AppendEntryMsg only */
    private Integer prevLogIdx;
    private Integer prevLogTerm;
    private LogEntry[] entries;
    private Integer committedIdx;
    /* << AppendEntryMsg only */

    /* >> RequestVoteMsg only */
    private Integer lastLogIdx;
    private Integer lastLogTerm;
    /* << RequestVoteMsg only */
}
