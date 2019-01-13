package com.amastigote.raftykv.procedure;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.RemoteCommunicationParamPack;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;

/**
 * @author: hwding
 * @date: 2018/12/18
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[REPLY APPEND PROC]")
public class ReplyAppendEntryResultProcedure implements Runnable {
    private final NodeState.FollowerAppendEntryResultContext resultContext;

    public ReplyAppendEntryResultProcedure(NodeState.FollowerAppendEntryResultContext resultContext) {
        this.resultContext = resultContext;
    }

    @Override
    public void run() {
        log.info("ReplyAppendEntryResultProcedure start...");
        GeneralMsg msg = new GeneralMsg();
        msg.setTerm(NodeState.currentTerm());
        msg.setMsgType(MsgType.HEARTBEAT);
        msg.setRpcAnalogType(MsgType.RpcAnalogType.RES);
        msg.setResponseToPort(NodeState.nodePort());
        msg.setLastReplicatedLogIdx(resultContext.getLastReplicatedLogIdx());

        Optional<RemoteCommunicationParamPack.RemoteTarget> targetOptional = NodeState
                .paramPack()
                .getCommunicationTargets()
                .parallelStream()
                .filter(target -> resultContext.getResToPort() == target.getPort())
                .findFirst();

        if (!targetOptional.isPresent()) {
            log.warn("no such target port {} in remote target list, give up responding to heartbeat", resultContext.getResToPort());
            return;
        }

        RemoteCommunicationParamPack.RemoteTarget target = targetOptional.get();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream outputStream;
        try {
            outputStream = new ObjectOutputStream(byteArrayOutputStream);
            outputStream.writeObject(msg);
            byte[] objBuf = byteArrayOutputStream.toByteArray();
            ByteBuf content = Unpooled.copiedBuffer(objBuf);
            DatagramPacket packet = new DatagramPacket(content, target.getSocketAddress(), RemoteCommunicationParamPack.senderAddr);

            target.getChannel().writeAndFlush(packet).addListener(future -> {
                if (!future.isSuccess()) {
                    log.error("failed to respond to {}", target.getPort());
                }
            });

        } catch (IOException e) {
            log.error("error when sending datagram", e);
        }

        log.info("ReplyAppendEntryResultProcedure end...");
    }
}
