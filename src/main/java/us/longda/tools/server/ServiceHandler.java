package us.longda.tools.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import us.longda.tools.proto.MessageEvent;
import us.longda.tools.proto.RpcWrapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

public class ServiceHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(
            ServiceHandler.class);
    
    private boolean debug;
    
    public ServiceHandler(boolean debug) {
        this.debug = debug;
    }

    public int recvRequest(RpcWrapper rpcRequest) throws Exception{
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
          throw e;

        }
        
        MessageEvent.Request request = null;
        try {
//               request = MessageEvent.Request.parseFrom(rpcRequest.pbData);
            //request = (MessageEvent.Request)Message.Builder.mergeFrom(rpcRequest.pbData).build();
            request = (MessageEvent.Request)defaultMessage.newBuilderForType().mergeFrom(rpcRequest.pbData).build();
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to parse protobuffer request ", e);
            throw e;
        }
        
        int cmdId = request.getCommandId();
        if (debug) {
            LOG.info("Server receive " + request.toString());
        }else if (cmdId % 100 == 0) {
            LOG.info("Server receive " + cmdId);
        }
        
        return cmdId;
    }
    
    public void sendResponse(ChannelHandlerContext ctx, int i) {
        MessageEvent.Response response = MessageEvent.Response.newBuilder()
                .setCommandId(i).setResponseStatus(0)
                .setResponseDataLength(0)
                .setResponseData(ByteString.copyFrom(new byte[0])).build();
        
        RpcWrapper rpcResponse = new RpcWrapper();
        
        rpcResponse.rpcTag = RpcWrapper.RPC_HEADER;
        
        //String typeName = response.getDescriptorForType().getFullName();
        String typeName = response.getClass().getName();
        rpcResponse.pbTypeName = typeName.getBytes();
        rpcResponse.pbTypeNameLen = rpcResponse.pbTypeName.length;
        
        rpcResponse.pbData = response.toByteArray();
        rpcResponse.pbDataLen = rpcResponse.pbData.length;
        
        rpcResponse.attachmentLen = 0;
        
        rpcResponse.rpcLength = rpcResponse.pbTypeNameLen
                + rpcResponse.pbDataLen + rpcResponse.attachmentLen
                + RpcWrapper.EXTRA_LEN;
        
        if (debug) {
            LOG.info("Server send response:" + i);
        }else if(i % 100 == 0) {
            LOG.info("Server send response:" + i);
        }
        
        
        ctx.write(rpcResponse);
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        
        if ((msg instanceof RpcWrapper) == false) {
            LOG.warn("Receive object isn't RpcWrapper:" + msg.getClass().getName());
            ctx.fireChannelRead(msg);
            return ;
        }
        
        int cmdId = recvRequest((RpcWrapper)msg);
        
        sendResponse(ctx, cmdId);
        
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
