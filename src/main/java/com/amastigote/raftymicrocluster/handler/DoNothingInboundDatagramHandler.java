package com.amastigote.raftymicrocluster.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[DO NOTHING HANDLER]")
@ChannelHandler.Sharable
public class DoNothingInboundDatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
        log.error("send channel recv data, ignored");
    }
}
