/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package c5db.control;

import c5db.C5ServerConstants;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.CommandReply;
import com.google.common.util.concurrent.AbstractService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.protostuff.Message;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Starts a HTTP service to listen for and respond to control messages.
 */
public class ControlService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(ControlService.class);

  private final C5Server server;
  private final Fiber serviceFiber;
  private final NioEventLoopGroup acceptConnectionGroup;
  private final NioEventLoopGroup ioWorkerGroup;
  private final int modulePort;
  private ServerBootstrap serverBootstrap;
  private Channel listenChannel;

  public ControlService(C5Server server,
                        Fiber serviceFiber,
                        NioEventLoopGroup acceptConnectionGroup,
                        NioEventLoopGroup ioWorkerGroup,
                        int modulePort) {
    this.server = server;
    this.serviceFiber = serviceFiber;
    this.acceptConnectionGroup = acceptConnectionGroup;
    this.ioWorkerGroup = ioWorkerGroup;
    this.modulePort = modulePort;
  }

  class MessageHandler extends SimpleChannelInboundHandler<CommandRpcRequest<? extends Message>> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CommandRpcRequest<? extends Message> msg) throws Exception {
      System.out.println("Server read off: " + msg);
      AsyncRequest.withOneReply(serviceFiber, server.getCommandRequests(), msg, reply -> {
        // got reply!
        System.out.println("Reply to client: " + reply);
        ctx.channel().writeAndFlush(reply);
      }, 1000, TimeUnit.MILLISECONDS, () -> {
        sendErrorReply(ctx.channel(), new Exception("Timed out request"));
      });
    }
  }

  private void sendErrorReply(Channel channel, Exception ex) {
    CommandReply reply = new CommandReply(false, "", ex.toString());
    channel.writeAndFlush(reply);
  }

  @Override
  protected void doStart() {
    serviceFiber.start();

    serviceFiber.execute(() -> {
      try {
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptConnectionGroup, ioWorkerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_BACKLOG, 100)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast("logger", new LoggingHandler(LogLevel.DEBUG));
                pipeline.addLast("http-server", new HttpServerCodec());
                pipeline.addLast("aggregator", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));


                pipeline.addLast("encode", new ServerHttpProtostuffEncoder());
                pipeline.addLast("decode", new ServerHttpProtostuffDecoder());

                pipeline.addLast("translate", new ServerDecodeCommandRequest());

                pipeline.addLast("inc-messages", new MessageHandler());
              }
            });

        serverBootstrap.bind(modulePort).addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              // yay
              listenChannel = future.channel();
              notifyStarted();
            } else {
              LOG.error("Unable to bind to port {}", modulePort);
              notifyFailed(future.cause());
            }
          }
        });
      } catch (Exception e) {
        notifyFailed(e);
      }

    });
  }

  @Override
  protected void doStop() {
    listenChannel.close();

    notifyStopped();
  }
}
