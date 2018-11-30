package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.CommunicateToOthersParamPack;
import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.ElectMsgType;
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

    private CommunicateToOthersParamPack paramPack;

    public RequestVoteProcedure() {
        this.paramPack = NodeStatus.paramPack();
    }

    @Override
    public void run() {
        log.info("reelection thread start...");

        for (int i = 0; i < this.paramPack.getDesChns().length; i++) {
            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setData(ElectMsgType.VOTE_REQ);
            msg.setTerm(NodeStatus.currentTerm());
            msg.setResponseToPort(NodeStatus.nodePort());

            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
                outputStream.writeObject(msg);
                byte[] objBuf = byteArrayOutputStream.toByteArray();
                ByteBuf content = Unpooled.copiedBuffer(objBuf);
                DatagramPacket packet = new DatagramPacket(content, this.paramPack.getDesAddrs()[i], this.paramPack.getSenderAddr());

                int finalI = i;
                this.paramPack.getDesChns()[i].writeAndFlush(packet).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.info("failed to req vote to " + this.paramPack.getDesPortsNum()[finalI]);
                        return;
                    }
                    log.info("vote req sent to " + this.paramPack.getDesPortsNum()[finalI]);
                });
            } catch (IOException e) {
                log.error("error when sending datagram", e);
            }
        }
    }
}
