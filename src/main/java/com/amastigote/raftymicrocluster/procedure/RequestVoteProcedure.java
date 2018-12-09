package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.RemoteCommunicationParamPack;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[REQUEST VOTE PROC]")
public class RequestVoteProcedure extends Thread {

    @Override
    public void run() {
        log.info("reelection thread start...");

        NodeStatus.paramPack().getCommunicationTargets().parallelStream().forEach(t -> {
            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setElectMsgType(MsgType.ElectMsgType.VOTE_REQ);
            msg.setTerm(NodeStatus.currentTerm());
            msg.setResponseToPort(NodeStatus.nodePort());

            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
                outputStream.writeObject(msg);
                byte[] objBuf = byteArrayOutputStream.toByteArray();
                ByteBuf content = Unpooled.copiedBuffer(objBuf);
                DatagramPacket packet = new DatagramPacket(content, t.getSocketAddress(), RemoteCommunicationParamPack.senderAddr);

                t.getChannel().writeAndFlush(packet).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.info("failed to req vote to {}", t.getPort());
                        return;
                    }
                    log.info("vote req sent to {}", t.getPort());
                });
            } catch (IOException e) {
                log.error("error when sending datagram", e);
            }
        });
    }
}
