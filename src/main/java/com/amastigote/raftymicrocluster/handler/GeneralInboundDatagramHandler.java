package com.amastigote.raftymicrocluster.handler;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.msg.ElectMsgDispatcher;
import com.amastigote.raftymicrocluster.handler.msg.HeartbeatMsgDispatcher;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
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

        log.info("udp datagram recv: {}", msg);

        /* reInit term if msg's term is higher */
        int compareToRecvTerm = this.termResetInvoker.apply(msg.getTerm());

        /* ignore any lower term heartbeat, mostly consider for the CANDIDATE situation */
        if ((compareToRecvTerm <= 0) && MsgType.ELECT.equals(msg.getMsgType())) {
            ElectMsgDispatcher.dispatch(msg, compareToRecvTerm, this.heartbeatWatchdogResetInvoker);
        }

        /* ignore any lower term heartbeat */
        if ((compareToRecvTerm <= 0) && MsgType.HEARTBEAT.equals(msg.getMsgType())) {
            HeartbeatMsgDispatcher.dispatch(this.heartbeatWatchdogResetInvoker);
        }
    }

    public static class HeartbeatWatchdogResetInvoker implements Function<Boolean, Void> {

        @Override
        public Void apply(Boolean needResetTimerIfAlreadyActive) {
            synchronized (NodeStatus.class) {
                if (!NodeStatus.heartBeatWatchdogThread().isAlive()) {
                    NodeStatus.rstHeartBeatWatchdogThread(true);
                    log.info("heartBeatWatchdogThread reset and start");
                } else if (needResetTimerIfAlreadyActive) {
                    NodeStatus.heartBeatWatchdogThread().interrupt();
                }
            }

            return null;
        }
    }

    public static class TermResetInvoker implements Function<Integer, Integer> {

        @Override
        public Integer apply(Integer term) {
            synchronized (NodeStatus.class) {

                /* -1: current is older
                 *  0: equal
                 * +1: current is newer */
                int compareToRecvTerm = Integer.compare(NodeStatus.currentTerm(), term);
                if (compareToRecvTerm == -1) {
                    log.info("newer term {} detected", term);

                    NodeStatus.updateTerm(term);
                    NodeStatus.rstVotedFor();
                }

                return compareToRecvTerm;
            }
        }
    }
}
