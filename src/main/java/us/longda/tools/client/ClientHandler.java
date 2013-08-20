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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import us.longda.tools.proto.MessageEvent;
import us.longda.tools.proto.MessageEvent.Request;
import us.longda.tools.proto.RpcWrapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Handler implementation for the echo client.  It initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(
            ClientHandler.class.getName());
    
    private AtomicInteger cmdId = new AtomicInteger(0);
    
    private boolean debug = false;
    private int     loopTimes = 1 * 1024 * 1024;
    private int     attachmentSize = 4 * 1024;
    private ByteBuffer attachBuffer;
    
    public ClientHandler(int attachmentSize, boolean debug, int loopTimes) {
        this.debug = debug;
        this.loopTimes = loopTimes;
        this.attachmentSize = attachmentSize;
        this.attachBuffer = ByteBuffer.allocate(attachmentSize);
        
        for (int i = 0; i < attachmentSize/4; i++) {
            attachBuffer.putInt(i);
        }
        
        attachBuffer.flip();
    }

    public void sendRequest(ChannelHandlerContext ctx, int i) {
        MessageEvent.Request request = MessageEvent.Request.newBuilder()
                .setCommandId(i)
                .setRequestDataLength(0)
                .setRequestData(ByteString.copyFrom(new byte[0]))
                .setAttachment(ByteString.copyFrom(new byte[0]))
                .build();
        
        RpcWrapper rpcRequest = new RpcWrapper();
        
        rpcRequest.rpcTag = RpcWrapper.RPC_HEADER;
        
        //String typeName = Request.getDescriptor().getName();
        String typeName = Request.class.getName();
        rpcRequest.pbTypeName = typeName.getBytes();
        rpcRequest.pbTypeNameLen = rpcRequest.pbTypeName.length;
        
        rpcRequest.pbData = request.toByteArray();
        rpcRequest.pbDataLen = rpcRequest.pbData.length;
        
        rpcRequest.attachment = attachBuffer.array();
        rpcRequest.attachmentLen = attachBuffer.limit();
        
        rpcRequest.rpcLength = rpcRequest.pbTypeNameLen +
                rpcRequest.pbDataLen +
                rpcRequest.attachmentLen +
                RpcWrapper.EXTRA_LEN;
        
        ctx.writeAndFlush(rpcRequest);
        
        if (debug) {
            LOG.info("Client send one request " + request);
        }else if (i % 100 == 0) {
            LOG.info("Client send request :" + i);
        }
    }
    
    public void recvResponse(RpcWrapper rpcRequest) {
        Message defaultMessage = null;
        String type = new String(rpcRequest.pbTypeName);
        try {
          Class<?> clazz = Class.forName(type);
          Method method = clazz.getDeclaredMethod("getDefaultInstance");
          defaultMessage = (Message) method.invoke(null);
        } catch (Exception e) {
          // We want to do the same thing with all exceptions. Not generally nice,
          // but this is slightly different.
          LOG.error("Failed to create request default instance", e);
          return ;

        }
        
        MessageEvent.Response response = null;
        try {
//               request = MessageEvent.Request.parseFrom(rpcRequest.pbData);
            //request = (MessageEvent.Request)Message.Builder.mergeFrom(rpcRequest.pbData).build();
            response = (MessageEvent.Response)defaultMessage.newBuilderForType().mergeFrom(rpcRequest.pbData).build();
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to parse protobuffer request ", e);
            return ;
        }
        
        int receiveCounter = response.getCommandId();
        if (receiveCounter != cmdId.get() ) {
            LOG.warn("Receive counter " + receiveCounter  + ", sendCounter:" + cmdId.get());
        }
        
        if (debug) {
            LOG.info("Client receive " + response);
        }if (receiveCounter % 100 == 0) {
            LOG.info("Client send request :" + receiveCounter);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        sendRequest(ctx, cmdId.incrementAndGet());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if ((msg instanceof RpcWrapper) == false) {
            LOG.warn("Receive object isn't RpcWrapper :" + msg.getClass().getName());
            return ;
        }
        
        recvResponse((RpcWrapper)msg);
        
        sendRequest(ctx, cmdId.incrementAndGet());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
       ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        LOG.error("Unexpected exception from downstream.", cause);
        ctx.close();
    }
}
