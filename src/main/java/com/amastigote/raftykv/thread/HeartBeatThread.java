package com.amastigote.raftykv.thread;


import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import com.amastigote.raftykv.protocol.TimeSpan;
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
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[HEARTBEAT THREAD]")
public class HeartBeatThread extends Thread {
    private ByteArrayOutputStream byteArrayOutputStream;
    private ObjectOutputStream outputStream;

    public HeartBeatThread() {
        byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            outputStream = new ObjectOutputStream(byteArrayOutputStream);
        } catch (IOException e) {
            log.error("error in construction", e);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void run() {
        while (!super.isInterrupted()) {
            try {
                NodeState.paramPack()
                        .getCommunicationTargets()
                        .forEach(target -> {
                            ByteBufOutputStream byteBufOutputStream = null;
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

                                outputStream.writeUnshared(msg);

                                ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(byteArrayOutputStream.size());
                                byteBufOutputStream = new ByteBufOutputStream(buf);
                                byteArrayOutputStream.writeTo(byteBufOutputStream);

                                final DatagramPacket packet = new DatagramPacket(buf, target.getSocketAddress(), RemoteIoParamPack.senderAddr);

                                target.getChannel().writeAndFlush(packet).addListener(future -> {
                                    if (!future.isSuccess()) {
                                        log.error("failed to send heartbeat to {}", targetPort);
                                        return;
                                    }
                                    log.debug("heartbeat sent to {}", targetPort);
                                });
                            } catch (IOException e) {
                                log.error("error when sending heartbeat, remote: {}", target, e);
                            } finally {
                                try {
                                    Objects.requireNonNull(byteBufOutputStream).close();
                                } catch (IOException e) {
                                    log.error("error when closing stream", e);
                                }
                            }
                        });
                Thread.sleep(TimeSpan.HEARTBEAT_SEND_INTERVAL);
            } catch (InterruptedException e) {
                log.info("heartbeat thread has been stopped, exit");
                break;
            } finally {
                try {
                    byteArrayOutputStream.close();
                    outputStream.close();
                } catch (IOException e) {
                    log.warn("error when closing stream", e);
                }
            }
        }
    }
}