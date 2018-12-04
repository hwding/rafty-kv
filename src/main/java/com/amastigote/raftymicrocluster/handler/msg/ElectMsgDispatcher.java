package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.procedure.VoteForCandidateProcedure;
import com.amastigote.raftymicrocluster.protocol.ElectMsgType;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.thread.HeartBeatThread;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[DISPATCHER ELECT-MSG]")
public final class ElectMsgDispatcher {

    public static void dispatch(GeneralMsg msg, boolean newerTerm, GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker) {
        ElectMsgType electMsgType = (ElectMsgType) msg.getData();

        if (electMsgType.equals(ElectMsgType.VOTE_REQ)) {
            NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
            if (NodeStatus.role().equals(Role.LEADER)) {

                /* step down and vote for this candidate */
                synchronized (NodeStatus.class) {
                    NodeStatus.setRoleTo(Role.FOLLOWER);
                    NodeStatus.heartbeatThread().interrupt();

                    heartbeatWatchdogResetInvoker.apply(false);

                    log.info("vote for newer term candidate and step down");
                }
                new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();
                return;
            }

            if (NodeStatus.role().equals(Role.CANDIDATE)) {
                if (newerTerm) {

                    /* step down and vote for this candidate */
                    synchronized (NodeStatus.class) {
                        NodeStatus.setRoleTo(Role.FOLLOWER);
                        NodeStatus.heartbeatThread().interrupt();

                        heartbeatWatchdogResetInvoker.apply(false);
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

                /* the follower should remain its state as long as it receives valid RPCs from leader OR **candidate** */
                heartbeatWatchdogResetInvoker.apply(true);
            }
            return;
        }

        if (electMsgType.equals(ElectMsgType.VOTE_RES)) {
            if (NodeStatus.role().equals(Role.CANDIDATE)) {
                int voteCnt = NodeStatus.incrVoteCnt();
                if (voteCnt >= NodeStatus.majorityNodeCnt()) {
                    log.info("nice, i'm leader now with voteCnt {}", voteCnt);
                    synchronized (NodeStatus.class) {
                        NodeStatus.setRoleTo(Role.LEADER);

                        if (!NodeStatus.heartbeatThread().isAlive()) {
                            NodeStatus.setHeartbeatThread(new HeartBeatThread());
                            NodeStatus.heartbeatThread().start();
                        }

                        NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                    }
                    NodeStatus.voteCntTimeoutDetectThread().interrupt();
                }
            }
            return;
        }
    }
}
