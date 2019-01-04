package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.NodeState;
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
@SuppressWarnings({"JavaDoc", "Duplicates"})
@Slf4j(topic = "[VOTE FOR CANDIDATE PROC]")
public class VoteForCandidateProcedure extends Thread {

    private int candidatePort;
    private int candidateTerm;
    private int candidateLastReplicatedLogIdx;
    private int candidateLastReplicatedLogTerm;

    public VoteForCandidateProcedure(
            int candidatePort,
            int candidateTerm,
            int candidateLastReplicatedLogIdx,
            int candidateLastReplicatedLogTerm) {
        this.candidatePort = candidatePort;
        this.candidateTerm = candidateTerm;
        this.candidateLastReplicatedLogIdx = candidateLastReplicatedLogIdx;
        this.candidateLastReplicatedLogTerm = candidateLastReplicatedLogTerm;
    }

    @Override
    public void run() {
        log.info("VoteForCandidateProcedure start...");

        synchronized (NodeState.class) {

            /* >> election safety check */
            final int currentLastReplicatedLogIdx = NodeState.lastReplicatedLogIdx();
            final int currentLastReplicatedLogTerm = NodeState.lastReplicatedLogTerm();

            if (currentLastReplicatedLogTerm > candidateLastReplicatedLogTerm) {
                log.info("candidate failed safety (up-to-date) check in voter, give up: " +
                                "currentLastReplicatedLogTerm {} > candidateLastReplicatedLogTerm {}",
                        currentLastReplicatedLogTerm, candidateLastReplicatedLogTerm);
                return;
            }

            if (currentLastReplicatedLogIdx > candidateLastReplicatedLogIdx) {
                log.info("candidate failed safety (up-to-date) check in voter, give up: " +
                                "currentLastReplicatedLogIdx {} > candidateLastReplicatedLogIdx {}",
                        currentLastReplicatedLogIdx, candidateLastReplicatedLogIdx);
                return;
            }
            /* >> election safety check */

            int votedFor = NodeState.votedFor();

            if (votedFor != 0) {
                log.warn("has voted for {} in term {}, give up", votedFor, candidateTerm);
                return;
            }

            if (NodeState.currentTerm() != candidateTerm) {
                log.warn("term updated {} -> {} before voting, give up", candidateTerm, NodeState.currentTerm());
                return;
            }

            Optional<RemoteCommunicationParamPack.RemoteTarget> targetOptional = NodeState
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

                    NodeState.voteFor(candidatePort);
                });

            } catch (IOException e) {
                log.error("error when sending datagram", e);
            }
        }
        log.info("VoteForCandidateProcedure end...");
    }
}
