package com.amastigote.raftykv.procedure;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import com.amastigote.raftykv.util.RemoteIoParamPack;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings({"JavaDoc", "Duplicates"})
@Slf4j(topic = "[REQUEST VOTE PROC]")
public class RequestVoteProcedure extends Thread {

    @Override
    public void run() {
        log.info(">> RequestVoteProcedure start");

        NodeState.paramPack().getCommunicationTargets().parallelStream().forEach(t -> {
            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setRpcAnalogType(MsgType.RpcAnalogType.REQ);
            msg.setTerm(NodeState.currentTerm());
            msg.setResponseToPort(NodeState.nodePort());

            msg.setLastLogIdx(NodeState.lastReplicatedLogIdx());
            msg.setLastLogTerm(NodeState.lastReplicatedLogTerm());

            ByteArrayOutputStream byteArrayOutputStream = null;
            ObjectOutputStream outputStream = null;
            ByteBufOutputStream byteBufOutputStream = null;
            try {
                byteArrayOutputStream = new ByteArrayOutputStream();
                outputStream = new ObjectOutputStream(byteArrayOutputStream);

                outputStream.writeUnshared(msg);

                ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(byteArrayOutputStream.size());
                byteBufOutputStream = new ByteBufOutputStream(buf);
                byteArrayOutputStream.writeTo(byteBufOutputStream);

                DatagramPacket packet = new DatagramPacket(buf, t.getSocketAddress(), RemoteIoParamPack.senderAddr);

                t.getChannel().writeAndFlush(packet).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.error("failed to req vote to {}", t.getPort());
                        return;
                    }
                    log.info("vote req sent to {}", t.getPort());
                });
            } catch (IOException e) {
                log.error("error when sending datagram", e);
            } finally {
                try {
                    Objects.requireNonNull(byteBufOutputStream).close();
                    Objects.requireNonNull(byteArrayOutputStream).close();
                    Objects.requireNonNull(outputStream).close();
                } catch (Exception e) {
                    log.warn("error when closing stream", e);
                }
            }
        });
        log.info("<< RequestVoteProcedure end");
    }
}
