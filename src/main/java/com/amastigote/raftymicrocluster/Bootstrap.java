package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.handler.DoNothingInboundDatagramHandler;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.thread.HeartBeatRecvTimeoutDetectThread;
import com.amastigote.raftymicrocluster.thread.HeartBeatThread;
import com.amastigote.raftymicrocluster.thread.VoteCntTimeoutDetectThread;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
/* be aware of components' initialize order! */
@Slf4j(topic = "[BOOTSTRAP]")
public class Bootstrap {

    public static void main(String[] args) {
        Random random = new Random(System.nanoTime());
        NodeStatus.init("Node-" + Integer.valueOf(args[0]), Integer.valueOf(args[0]), args.length);
        EventLoopGroup loopGroup = new NioEventLoopGroup();
        DoNothingInboundDatagramHandler doNothingInboundDatagramHandler = new DoNothingInboundDatagramHandler();

        /* init leader */
        String[] desPorts = new String[args.length - 1];
        System.arraycopy(args, 1, desPorts, 0, args.length - 1);

        log.info("node @ {}, remote @ {}", NodeStatus.nodePort(), Arrays.toString(desPorts));

        int[] desPortsNum = Stream
                .of(desPorts)
                .mapToInt(Integer::valueOf)
                .toArray();
        Channel[] desChns = new Channel[desPortsNum.length];

        for (int i = 0; i < desChns.length; ++i) {
            try {
                desChns[i] = new io.netty.bootstrap.Bootstrap()
                        .group(loopGroup)
                        .channel(NioDatagramChannel.class)
                        .handler(doNothingInboundDatagramHandler)
                        .bind(8100 + random.nextInt(100))
                        .sync()
                        .channel();
            } catch (Exception e) {
                --i;
                log.warn("port already bind? retry...");
            }
        }

        CommunicateToOthersParamPack paramPack = new CommunicateToOthersParamPack(desChns, desPortsNum);
        NodeStatus.setParamPack(paramPack);

        Thread heartBeatRecvTimeoutDetectThread = new HeartBeatRecvTimeoutDetectThread();
        Thread voteCntTimeoutDetectThread = new VoteCntTimeoutDetectThread();
        Thread heartBeatThread = new HeartBeatThread();

        NodeStatus.setHeartbeatThread(heartBeatThread);
        NodeStatus.setVoteCntTimeoutDetectThread(voteCntTimeoutDetectThread);
        NodeStatus.setHeartbeatRecvTimeoutDetectThread(heartBeatRecvTimeoutDetectThread);

        GeneralInboundDatagramHandler generalInboundDatagramHandler = new GeneralInboundDatagramHandler();

        NodeStatus.heartbeatRecvTimeoutDetectThread().start();

        new io.netty.bootstrap.Bootstrap()
                .group(loopGroup)
                .channel(NioDatagramChannel.class)
                .handler(generalInboundDatagramHandler)
                .bind(NodeStatus.nodePort());
    }
}
