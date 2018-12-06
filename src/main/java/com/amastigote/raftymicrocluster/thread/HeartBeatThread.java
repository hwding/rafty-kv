package com.amastigote.raftymicrocluster.thread;


import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.RemoteCommunicationParamPack;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import com.amastigote.raftymicrocluster.protocol.TimeSpan;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[HEARTBEAT THREAD]")
public class HeartBeatThread extends Thread {

    /* TODO job list here */

    @Override
    public void run() {
        GeneralMsg msg = new GeneralMsg();
        msg.setMsgType(MsgType.HEARTBEAT);
        msg.setTerm(NodeStatus.currentTerm());

        while (!super.isInterrupted()) {
            try {
                NodeStatus.paramPack()
                        .getCommunicationTargets()
                        .parallelStream()
                        .forEach(target -> {
                            try {
                                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
                                outputStream.writeObject(msg);
                                byte[] objBuf = byteArrayOutputStream.toByteArray();

                                ByteBuf content = Unpooled.copiedBuffer(objBuf);
                                DatagramPacket packet = new DatagramPacket(content, target.getSocketAddress(), RemoteCommunicationParamPack.senderAddr);

                                target.getChannel().writeAndFlush(packet).addListener(future -> {
                                    if (!future.isSuccess()) {
                                        log.info("failed to send heartbeat to {}", target.getPort());
                                        return;
                                    }
                                    log.info("heartbeat sent to {}", target.getPort());
                                });
                            } catch (IOException e) {
                                log.error("", e);
                            }
                        });
                Thread.sleep(TimeSpan.HEARTBEAT_SEND_INTERVAL);
            } catch (InterruptedException e) {
                log.info("heartbeat thread has been stopped, exit");
                break;
            }
        }
    }
}