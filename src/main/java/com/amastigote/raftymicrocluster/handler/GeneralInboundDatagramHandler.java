package com.amastigote.raftymicrocluster.handler;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.procedure.VoteForCandidateProcedure;
import com.amastigote.raftymicrocluster.protocol.ElectMsgType;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.thread.HeartBeatRecvTimeoutDetectThread;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[MSG HANDLER]")
public class GeneralInboundDatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) throws Exception {

        final ByteBuf byteBuf = datagramPacket.content();
        final byte[] bytes = new byte[byteBuf.readableBytes()];

        byteBuf.readBytes(bytes);

        /* deserialize GeneralMsg */
        GeneralMsg msg;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream stream = new ObjectInputStream(byteArrayInputStream);
        msg = (GeneralMsg) stream.readObject();

        log.info("udp pack recv: {}", msg);

        /* reInit term if necessary */
        boolean newerTerm = false;
        synchronized (NodeStatus.class) {
            if (NodeStatus.currentTerm() < msg.getTerm()) {
                NodeStatus.reInitTerm(msg.getTerm());
                newerTerm = true;
            }
        }

        if (MsgType.ELECT.equals(msg.getMsgType())) {
            ElectMsgType electMsgType = (ElectMsgType) msg.getData();

            if (electMsgType.equals(ElectMsgType.VOTE_REQ)) {
                NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                if (NodeStatus.role().equals(Role.LEADER)) {

                    /* step down and vote for this candidate */
                    synchronized (NodeStatus.class) {
                        NodeStatus.setRoleTo(Role.FOLLOWER);
                        NodeStatus.heartbeatThread().interrupt();

                        if (!NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                            NodeStatus.setHeartbeatRecvTimeoutDetectThread(new HeartBeatRecvTimeoutDetectThread());
                            NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                        }

                        log.info("vote for newer term candidate and step down");
                    }
                    new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();
                    return;
                }

                if (NodeStatus.role().equals(Role.CANDIDATE)) {
                    if (newerTerm) {

                        /* step down and vote for this candidate */
                        //noinspection Duplicates
                        synchronized (NodeStatus.class) {
                            NodeStatus.setRoleTo(Role.FOLLOWER);
                            NodeStatus.heartbeatThread().interrupt();

                            if (NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                                NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                            } else {
                                NodeStatus.setHeartbeatRecvTimeoutDetectThread(new HeartBeatRecvTimeoutDetectThread());
                                NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                            }
                        }
                        new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();

                        log.info("vote for newer term candidate and step down");
                        return;
                    }
                    log.info("competitor's same term vote req, do nothing");
                    return;
                }

                if (NodeStatus.role().equals(Role.FOLLOWER) && newerTerm) {
                    new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();

                    //noinspection Duplicates
                    if (NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                        NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                    } else {
                        synchronized (NodeStatus.class) {
                            if (!NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                                NodeStatus.setHeartbeatRecvTimeoutDetectThread(new HeartBeatRecvTimeoutDetectThread());
                                NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                            }
                        }
                    }

                    return;
                }
            }

            if (electMsgType.equals(ElectMsgType.VOTE_RES)) {
                if (NodeStatus.role().equals(Role.CANDIDATE)) {
                    int voteCnt = NodeStatus.incrVoteCnt();
                    if (voteCnt >= NodeStatus.majorityNodeCnt()) {
                        NodeStatus.voteCntTimeoutDetectThread().interrupt();
                    }
                }
                return;
            }
        }

        if (MsgType.HEARTBEAT.equals(msg.getMsgType())) {

            if (NodeStatus.role().equals(Role.LEADER) && msg.getTerm() > NodeStatus.currentTerm()) {
                NodeStatus.reInitTerm(msg.getTerm());

                /* step down */
                //noinspection Duplicates
                synchronized (NodeStatus.class) {
                    NodeStatus.setRoleTo(Role.FOLLOWER);
                    NodeStatus.heartbeatThread().interrupt();

                    if (NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                        NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                    } else {
                        NodeStatus.setHeartbeatRecvTimeoutDetectThread(new HeartBeatRecvTimeoutDetectThread());
                        NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                    }
                }

                return;
            }

            /* reset heartbeat timer if not alive */
            if (!NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                log.info("hbRecvTimeoutDetThread is not alive, restart");

                synchronized (NodeStatus.class) {
                    /* recheck */
                    if (!NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                        NodeStatus.setHeartbeatRecvTimeoutDetectThread(new HeartBeatRecvTimeoutDetectThread());
                        NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                    } else {
                        NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                    }
                }
                return;
            }

            /* standard heartbeat recv condition */
            NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
        }
    }
}
