package com.amastigote.raftykv.util;

import com.amastigote.raftykv.NodeState;
import io.netty.channel.Channel;
import io.netty.util.internal.SocketUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings({"JavaDoc"})
@Getter
public class RemoteIoParamPack {
    public static final InetSocketAddress senderAddr = SocketUtils.socketAddress(
            "localhost", NodeState.nodePort()
    );
    private List<RemoteTarget> communicationTargets;

    public RemoteIoParamPack(List<RemoteTarget> communicationTargets) {
        this.communicationTargets = communicationTargets;
    }

    @Getter
    @AllArgsConstructor
    public static class RemoteTarget {
        private int port;
        private Channel channel;
        private InetSocketAddress socketAddress;
    }
}
