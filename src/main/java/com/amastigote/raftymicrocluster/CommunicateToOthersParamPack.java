package com.amastigote.raftymicrocluster;

import io.netty.channel.Channel;
import io.netty.util.internal.SocketUtils;
import lombok.Getter;

import java.net.InetSocketAddress;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings({"JavaDoc", "MismatchedReadAndWriteOfArray"})
@Getter
public class CommunicateToOthersParamPack {
    private Channel[] desChns;
    private int[] desPortsNum;
    private InetSocketAddress[] desAddrs;
    private InetSocketAddress senderAddr = SocketUtils.socketAddress("localhost", NodeStatus.nodePort());

    CommunicateToOthersParamPack(Channel[] desChns, int[] remoteNodePorts) {
        this.desChns = desChns;
        this.desPortsNum = remoteNodePorts;
        desAddrs = new InetSocketAddress[desPortsNum.length];
        for (int i = 0; i < desPortsNum.length; ++i) {
            desAddrs[i] = new InetSocketAddress("localhost", this.desPortsNum[i]);
        }
    }
}
