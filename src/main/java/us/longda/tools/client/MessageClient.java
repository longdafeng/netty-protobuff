/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package us.longda.tools.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import us.longda.tools.proto.RpcWrapperMsgDecoder;
import us.longda.tools.proto.RpcWrapperMsgEncoder;

/**
 * Sends one message when a connection is open and echoes back any received
 * data to the server.  Simply put, the echo client initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public class MessageClient {

    private Properties properties;
    private String host;
    private int port;
    private boolean debug = false;
    private int     loopTimes = 1 * 1024 * 1024;
    private int     attachmentSize = 4 * 1024;
    
    private final String configFile;
    
    public MessageClient(final String configFile) {
        this.configFile = configFile;
    }
    
    public void init() throws Exception {
        
        InputStream fileStream = new FileInputStream(configFile);
        
        properties = new Properties();
        properties.load(fileStream);
        
        host = properties.getProperty("server", "localhost");
        port = Integer.valueOf(properties.getProperty("port", "9000"));
        debug = Boolean.valueOf(properties.getProperty("debug", "false"));
        loopTimes = Integer.valueOf(properties.getProperty("looptimes","1048576"));
        attachmentSize = Integer.valueOf(properties.getProperty("attachment.size", "4096"));
    }


    public void run() throws Exception {
        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.TCP_NODELAY, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(
                             new RpcWrapperMsgDecoder(),
                             new RpcWrapperMsgEncoder(),
                             new ClientHandler(attachmentSize, debug, loopTimes));
                 }
             });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        // Print usage if no argument is specified.
        if (args.length < 1) {
            System.err.println(
                    "Usage: " + MessageClient.class.getSimpleName() +
                    " <host> <port> [<first message size>]");
            return;
        }

        MessageClient client = new MessageClient(args[0]);
        client.init();
        client.run();

    }
}
