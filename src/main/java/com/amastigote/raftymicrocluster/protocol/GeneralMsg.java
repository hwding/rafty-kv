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
    private Object data;
    private Integer term;
    private Integer responseToPort;
}
