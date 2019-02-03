package com.amastigote.raftykv.handler;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.handler.msg.ElectMsgDispatcher;
import com.amastigote.raftykv.handler.msg.HeartbeatMsgDispatcher;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ObjectInputStream;
import java.util.function.Function;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[MSG HANDLER]")
public class GeneralInboundDatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private final HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker = new HeartbeatWatchdogResetInvoker();
    private final TermResetInvoker termResetInvoker = new TermResetInvoker();

    /* be aware of the msg is auto released in outer wrapper method channelRead() */
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) throws Exception {

        final ByteBuf byteBuf = datagramPacket.content();
        final ByteBufInputStream byteBufInputStream = new ByteBufInputStream(byteBuf);

        /* deserialize GeneralMsg */
        GeneralMsg msg;
        ObjectInputStream stream = new ObjectInputStream(byteBufInputStream);
        msg = (GeneralMsg) stream.readUnshared();

        log.debug("udp datagram recv: {}", msg);

        byteBufInputStream.close();
        stream.close();

        /* reInit term if msg's term is higher */
        int compareToRecvTerm = this.termResetInvoker.apply(msg.getTerm());

        /* ignore any lower term heartbeat */
        if (compareToRecvTerm > 0) {
            return;
        }

        if (MsgType.ELECT.equals(msg.getMsgType())) {
            ElectMsgDispatcher.dispatch(msg, compareToRecvTerm, this.heartbeatWatchdogResetInvoker);
        }

        if (MsgType.HEARTBEAT.equals(msg.getMsgType())) {
            HeartbeatMsgDispatcher.dispatch(msg, this.heartbeatWatchdogResetInvoker);
        }
    }

    public static class HeartbeatWatchdogResetInvoker implements Function<Boolean, Void> {

        @Override
        public Void apply(Boolean needResetTimerIfAlreadyActive) {
            synchronized (NodeState.class) {
                if (!NodeState.heartBeatWatchdogThread().isAlive()) {
                    NodeState.rstHeartBeatWatchdogThread(true);
                    log.debug("heartBeatWatchdogThread reset and start");
                } else if (needResetTimerIfAlreadyActive) {
                    NodeState.heartBeatWatchdogThread().interrupt();
                }
            }

            return null;
        }
    }

    public static class TermResetInvoker implements Function<Integer, Integer> {

        @Override
        public Integer apply(Integer term) {
            synchronized (NodeState.class) {

                /* -1: current is older
                 *  0: equal
                 * +1: current is newer */
                int compareToRecvTerm = Integer.compare(NodeState.currentTerm(), term);
                if (compareToRecvTerm == -1) {
                    log.info("newer term {} detected", term);

                    NodeState.updateTerm(term);
                    NodeState.rstVotedFor();
                }

                return compareToRecvTerm;
            }
        }
    }
}
