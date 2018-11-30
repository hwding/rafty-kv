package com.amastigote.raftymicrocluster.thread;


import com.amastigote.raftymicrocluster.CommunicateToOthersParamPack;
import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[HEARTBEAT THREAD]")
public class HeartBeatThread extends Thread {

    private CommunicateToOthersParamPack paramPack;

    /* TODO job list here */

    public HeartBeatThread() {
        this.paramPack = NodeStatus.paramPack();
    }

    @Override
    public void run() {
        log.info("HB start target port {}", Arrays.toString(this.paramPack.getDesPortsNum()));

        GeneralMsg msg = new GeneralMsg();
        msg.setMsgType(MsgType.HEARTBEAT);
        msg.setTerm(NodeStatus.currentTerm());

        while (!super.isInterrupted()) {
            try {
                for (int i = 0; i < this.paramPack.getDesChns().length; i++) {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
                    outputStream.writeObject(msg);
                    byte[] objBuf = byteArrayOutputStream.toByteArray();

                    ByteBuf content = Unpooled.copiedBuffer(objBuf);
                    DatagramPacket packet = new DatagramPacket(content, this.paramPack.getDesAddrs()[i], this.paramPack.getSenderAddr());
                    int finalI = i;
                    this.paramPack.getDesChns()[i].writeAndFlush(packet).addListener(future -> {
                        if (!future.isSuccess()) {
                            log.info("failed to send heartbeat to " + this.paramPack.getDesChns()[finalI]);
                            return;
                        }
                        log.info("heartbeat sent to " + this.paramPack.getDesChns()[finalI]);
                    });
                }
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error(null, e);
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

//        for (int i = 0; i < this.paramPack.getDesChns().length; i++) {
//            int finalI = i;
//            this.paramPack.getDesChns()[i]
//                    .closeFuture()
//                    .addListener(future -> log.debug("heartbeat channel shutdown for " + this.paramPack.getDesPortsNum()[finalI]));
//        }
    }
}
