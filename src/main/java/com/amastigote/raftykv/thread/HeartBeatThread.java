package com.amastigote.raftykv.thread;


import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import com.amastigote.raftykv.protocol.TimeSpan;
import com.amastigote.raftykv.util.RemoteIoParamPack;
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

    @Override
    public void run() {
        while (!super.isInterrupted()) {
            log.info("round start...");
            try {
                NodeState.paramPack()
                        .getCommunicationTargets()
                        .parallelStream()
                        .forEach(target -> {
                            try {
                                final int targetPort = target.getPort();

                                final GeneralMsg msg = new GeneralMsg();
                                msg.setMsgType(MsgType.HEARTBEAT);
                                msg.setRpcAnalogType(MsgType.RpcAnalogType.REQ);
                                msg.setTerm(NodeState.currentTerm());
                                msg.setResponseToPort(NodeState.nodePort());

                                NodeState.FollowerResidualEntryInfo residualLogs = NodeState.genResidualEntryInfoForFollower(targetPort);
                                msg.setEntries(residualLogs.getResidualLogs());
                                msg.setLeaderCommittedIdx(NodeState.leaderCommittedIdx());
                                msg.setPrevLogIdx(residualLogs.getPrevLogIdx());
                                msg.setPrevLogTerm(residualLogs.getPrevLogTerm());

                                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
                                outputStream.writeObject(msg);
                                byte[] objBuf = byteArrayOutputStream.toByteArray();

                                ByteBuf content = Unpooled.copiedBuffer(objBuf);
                                DatagramPacket packet = new DatagramPacket(content, target.getSocketAddress(), RemoteIoParamPack.senderAddr);

                                target.getChannel().writeAndFlush(packet).addListener(future -> {
                                    if (!future.isSuccess()) {
                                        log.error("failed to send heartbeat to {}", targetPort);
                                        return;
                                    }
                                    log.info("heartbeat sent to {}", targetPort);
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
            log.info("round end...");
        }
    }
}