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
        log.info(">> VoteForCandidateProcedure start");

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

            Optional<RemoteIoParamPack.RemoteTarget> targetOptional = NodeState
                    .paramPack()
                    .getCommunicationTargets()
                    .parallelStream()
                    .filter(target -> candidatePort == target.getPort())
                    .findFirst();

            if (!targetOptional.isPresent()) {
                log.warn("no such target port {} in remote target list, give up voting", candidatePort);
                return;
            }

            RemoteIoParamPack.RemoteTarget target = targetOptional.get();

            GeneralMsg msg = new GeneralMsg();
            msg.setMsgType(MsgType.ELECT);
            msg.setRpcAnalogType(MsgType.RpcAnalogType.RES);

            msg.setTerm(candidateTerm);

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

                DatagramPacket packet = new DatagramPacket(buf, target.getSocketAddress(), RemoteIoParamPack.senderAddr);

                target.getChannel().writeAndFlush(packet).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.error("failed to vote for {}", target.getPort());
                        return;
                    }

                    NodeState.voteFor(candidatePort);
                });

            } catch (IOException e) {
                log.error("error when sending datagram", e);
            } finally {
                try {
                    Objects.requireNonNull(byteBufOutputStream).close();
                    Objects.requireNonNull(byteArrayOutputStream).close();
                    Objects.requireNonNull(outputStream).close();
                } catch (IOException e) {
                    log.warn("error when closing stream", e);
                }
            }
        }
        log.info("<< VoteForCandidateProcedure end");
    }
}
