package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.conf.NodeGlobalConf;
import com.amastigote.raftymicrocluster.handler.ClientHttpHandler;
import com.amastigote.raftymicrocluster.handler.DoNothingInboundDatagramHandler;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.protocol.TimeSpan;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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

        NodeGlobalConf.init();
        TimeSpan.init();
        NodeState.init(Integer.valueOf(args[0]), args.length);

        EventLoopGroup loopGroup = new NioEventLoopGroup(1);
        DoNothingInboundDatagramHandler doNothingInboundDatagramHandler = new DoNothingInboundDatagramHandler();

        String[] desPorts = new String[args.length - 1];
        System.arraycopy(args, 1, desPorts, 0, args.length - 1);

        log.info("node @ {}, remote @ {}", NodeState.nodePort(), Arrays.toString(desPorts));

        final int maxRetry = Integer.valueOf(NodeGlobalConf.readConf(NodeGlobalConf.KEY_BIND_RAND_PORT_MAX_RETRY));
        final int randPortMin = Integer.valueOf(NodeGlobalConf.readConf(NodeGlobalConf.KEY_RAND_PORT_MIN));
        final int randPortMax = Integer.valueOf(NodeGlobalConf.readConf(NodeGlobalConf.KEY_RAND_PORT_MAX));
        final int randBound = randPortMax - randPortMin;

        if (randBound <= 0) {
            log.error("rand port max {} must be greater than min {}", randPortMax, randPortMin);
            System.exit(-1);
        }

        Random random = new Random();
        List<RemoteCommunicationParamPack.RemoteTarget> communicationTargets = Stream
                .of(desPorts)
                .mapToInt(Integer::valueOf)
                .mapToObj(port -> {
                    int curRetry = maxRetry;
                    while (curRetry > 0) {
                        int randPort = randPortMin + random.nextInt(randPortMax - randPortMin);
                        try {
                            Channel remoteChn = new io.netty.bootstrap.Bootstrap()
                                    .group(loopGroup)
                                    .channel(NioDatagramChannel.class)
                                    .handler(doNothingInboundDatagramHandler)
                                    .bind(randPort)
                                    .sync()
                                    .channel();
                            InetSocketAddress remoteAddr = new InetSocketAddress("localhost", port);

                            return new RemoteCommunicationParamPack.RemoteTarget(
                                    port, remoteChn, remoteAddr
                            );
                        } catch (Exception e) {
                            log.warn("port {} occupied? retry...", randPort);
                            --curRetry;
                        }
                    }
                    log.error("failed to bind random port for remote node channels after {} retries", maxRetry);

                    System.exit(-1);
                    return null;
                })
                .collect(Collectors.toList());

        RemoteCommunicationParamPack paramPack = new RemoteCommunicationParamPack(communicationTargets);
        NodeState.initParamPack(paramPack);
        NodeState.initFollowerReplicatedIdxMap();

        NodeState.rstHeartbeatThread(false);
        NodeState.rstVoteResWatchdogThread(false);

        GeneralInboundDatagramHandler generalInboundDatagramHandler = new GeneralInboundDatagramHandler();

        new io.netty.bootstrap.Bootstrap()
                .group(loopGroup)
                .channel(NioDatagramChannel.class)
                .handler(generalInboundDatagramHandler)
                .bind(NodeState.nodePort());

        NodeState.rstHeartBeatWatchdogThread(true);

        /* >> server for client init */
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(NodeState.nodePort() + 200), 0);
            server.createContext("/", new ClientHttpHandler());
            server.start();
        } catch (IOException e) {
            log.error("fatal: cannot bind port for http server");
            System.exit(-1);
        }
        /* << server for client init */

        log.info("init ok");
    }
}
