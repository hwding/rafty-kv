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
import java.util.Optional;

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
        log.info("VoteForCandidateProcedure start...");

        int votedFor = NodeStatus.votedFor();

        if (votedFor != 0) {
            log.warn("has voted for {} in term {}, give up", votedFor, candidateTerm);
            return;
        }

        synchronized (NodeStatus.class) {
            votedFor = NodeStatus.votedFor();

            /* double check */
            if (votedFor != 0) {
                log.warn("has voted for {} in term {}, give up", votedFor, candidateTerm);
                return;
            }

            if (NodeStatus.currentTerm() != candidateTerm) {
                log.warn("term updated {} -> {} before voting, give up", candidateTerm, NodeStatus.currentTerm());
                return;
            }

            Optional<RemoteCommunicationParamPack.RemoteTarget> targetOptional = NodeStatus
                    .paramPack()
                    .getCommunicationTargets()
                    .parallelStream()
                    .filter(target -> candidatePort == target.getPort())
                    .findFirst();

            if (!targetOptional.isPresent()) {
                log.warn("no such target port {} in remote target list, give up voting", candidatePort);
                return;
            }

            RemoteCommunicationParamPack.RemoteTarget target = targetOptional.get();

            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setRpcAnalogType(MsgType.RpcAnalogType.RES);

            msg.setTerm(candidateTerm);

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
                        log.error("failed to vote for {}", target.getPort());
                        return;
                    }

                    NodeStatus.voteFor(candidatePort);
                });

            } catch (IOException e) {
                log.error("error when sending datagram", e);
            }
        }
        log.info("VoteForCandidateProcedure end...");
    }
}
