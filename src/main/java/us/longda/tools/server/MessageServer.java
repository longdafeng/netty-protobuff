package us.longda.tools.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import us.longda.tools.proto.RpcWrapperMsgDecoder;
import us.longda.tools.proto.RpcWrapperMsgEncoder;

public class MessageServer implements Runnable {
    private static final Logger     LOG         = Logger.getLogger(MessageServer.class);
    
    private final String configFile; 
    
    private Properties properties;
    
    private int port;
    
    private int bufSize;
    
    private boolean debug;

    public MessageServer(final String configFile) {
        this.configFile = configFile;
    }
    
    public void init() throws Exception {
        
        InputStream fileStream = new FileInputStream(configFile);
        
        properties = new Properties();
        properties.load(fileStream);
        
        port = Integer.valueOf(properties.getProperty("port", "9000"));
        bufSize = Integer.valueOf(properties.getProperty("network.buffer.size", "1048576"));
        debug = Boolean.valueOf(properties.getProperty("debug", "false"));
    }

    public void run() {
        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(
                             new RpcWrapperMsgDecoder(),
                             new RpcWrapperMsgEncoder(),
                             new ServiceHandler(debug));
                 }
             });

            // Start the server.
            ChannelFuture f = b.bind(port).sync();

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Invalid parameter, please input the config file path");
            System.exit(-1);
        }
        
        MessageServer server = new MessageServer(args[0]);
        
        server.init();
        
        server.run();
        
    }
    
}