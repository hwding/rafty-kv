package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.ElectMsgType;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import com.amastigote.raftymicrocluster.protocol.Role;
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
@Slf4j(topic = "[VOTE FOR CANDIDATE PROC]")
public class VoteForCandidateProcedure extends Thread {

    private int candidatePort;
    private int candidateTerm;

    public VoteForCandidateProcedure(int candidatePort, int candidateTerm) {
        this.candidatePort = candidatePort;
        this.candidateTerm = candidateTerm;
    }

    @Override
    public void run() {
        NodeStatus.setRoleTo(Role.FOLLOWER);

        if (candidateTerm <= NodeStatus.votedTerm()) {
            log.warn("has voted in term " + candidateTerm, " give up");
            return;
        }

        synchronized (NodeStatus.class) {

            /* double check */
            if (candidateTerm <= NodeStatus.votedTerm()) {
                log.warn("has voted in term " + candidateTerm, " give up");
                return;
            }

            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setData(ElectMsgType.VOTE_RES);
            msg.setTerm(candidateTerm);

            int desIdx = -1;
            for (int i = 0; i < NodeStatus.paramPack().getDesPortsNum().length; i++) {
                if (candidatePort == NodeStatus.paramPack().getDesPortsNum()[i]) {
                    desIdx = i;
                    break;
                }
            }
            if (desIdx == -1) {
                log.warn("no such node {}, give up voting", candidatePort);
                return;
            }

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream outputStream;
            try {
                outputStream = new ObjectOutputStream(byteArrayOutputStream);
                outputStream.writeObject(msg);
                byte[] objBuf = byteArrayOutputStream.toByteArray();
                ByteBuf content = Unpooled.copiedBuffer(objBuf);
                DatagramPacket packet = new DatagramPacket(content, NodeStatus.paramPack().getDesAddrs()[desIdx], NodeStatus.paramPack().getSenderAddr());

                int finalDesIdx = desIdx;
                NodeStatus.paramPack().getDesChns()[desIdx].writeAndFlush(packet).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.info("failed to vote for " + NodeStatus.paramPack().getDesPortsNum()[finalDesIdx]);
                        return;
                    }
                    log.info("voted for " + NodeStatus.paramPack().getDesPortsNum()[finalDesIdx]);
                });

                NodeStatus.updateVotedTerm(candidateTerm);
            } catch (IOException e) {
                log.error("error when sending datagram", e);
            }
        }
    }
}
