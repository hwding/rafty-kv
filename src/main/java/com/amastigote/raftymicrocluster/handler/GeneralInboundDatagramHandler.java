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
        boolean newerTerm = this.termResetInvoker.apply(msg.getTerm());

        if (MsgType.ELECT.equals(msg.getMsgType())) {
            ElectMsgDispatcher.dispatch(msg, newerTerm, this.heartbeatWatchdogResetInvoker);
        }

        /* ignore any lower term heartbeat, mostly consider for the CANDIDATE situation */
        if (newerTerm && MsgType.HEARTBEAT.equals(msg.getMsgType())) {
            HeartbeatMsgDispatcher.dispatch(this.heartbeatWatchdogResetInvoker);
        }
    }

    public static class HeartbeatWatchdogResetInvoker implements Function<Boolean, Void> {

        @Override
        public Void apply(Boolean needResetTimerIfAlreadyActive) {
            synchronized (NodeStatus.class) {
                if (!NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                    NodeStatus.resetHeartbeatRecvTimeoutDetectThread(true);
                    log.info("heartbeatRecvTimeoutDetectThread reset and start");
                } else if (needResetTimerIfAlreadyActive) {
                    NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                }
            }

            return null;
        }
    }

    public static class TermResetInvoker implements Function<Integer, Boolean> {

        @Override
        public Boolean apply(Integer term) {
            if (NodeStatus.currentTerm() <= term) {
                synchronized (NodeStatus.class) {
                    if (NodeStatus.currentTerm() <= term) {
                        log.info("equal or newer term detected");

                        NodeStatus.updateTerm(term);
                        NodeStatus.resetVotedFor();
                        return true;
                    }
                    return false;
                }
            }

            return false;
        }
    }
}
