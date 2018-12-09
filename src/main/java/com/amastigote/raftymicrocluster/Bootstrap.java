package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.handler.DoNothingInboundDatagramHandler;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
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
        log.info("init...");

        Random random = new Random(System.nanoTime());
        NodeStatus.init("Node-" + Integer.valueOf(args[0]), Integer.valueOf(args[0]), args.length);
        EventLoopGroup loopGroup = new NioEventLoopGroup();
        DoNothingInboundDatagramHandler doNothingInboundDatagramHandler = new DoNothingInboundDatagramHandler();

        String[] desPorts = new String[args.length - 1];
        System.arraycopy(args, 1, desPorts, 0, args.length - 1);

        log.info("node @ {}, remote @ {}", NodeStatus.nodePort(), Arrays.toString(desPorts));

        List<RemoteCommunicationParamPack.RemoteTarget> communicationTargets = Stream
                .of(desPorts)
                .mapToInt(Integer::valueOf)
                .mapToObj(port -> {
                    while (true) {
                        try {
                            Channel remoteChn = new io.netty.bootstrap.Bootstrap()
                                    .group(loopGroup)
                                    .channel(NioDatagramChannel.class)
                                    .handler(doNothingInboundDatagramHandler)
                                    .bind(8100 + random.nextInt(100))
                                    .sync()
                                    .channel();
                            InetSocketAddress remoteAddr = new InetSocketAddress("localhost", port);

                            return new RemoteCommunicationParamPack.RemoteTarget(
                                    port, remoteChn, remoteAddr
                            );
                        } catch (Exception e) {
                            log.warn("port already bind? retry...");
                        }
                    }
                })
                .collect(Collectors.toList());

        RemoteCommunicationParamPack paramPack = new RemoteCommunicationParamPack(communicationTargets);
        NodeStatus.setParamPack(paramPack);

        NodeStatus.resetHeartbeatThread(false);
        NodeStatus.resetVoteResWatchdogThread(false);

        GeneralInboundDatagramHandler generalInboundDatagramHandler = new GeneralInboundDatagramHandler();

        new io.netty.bootstrap.Bootstrap()
                .group(loopGroup)
                .channel(NioDatagramChannel.class)
                .handler(generalInboundDatagramHandler)
                .bind(NodeStatus.nodePort());

        NodeStatus.resetHeartBeatWatchdogThread(true);

        log.info("init ok");
    }
}
