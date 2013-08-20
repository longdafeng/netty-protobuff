package us.longda.tools.proto;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class RpcWrapperMsgEncoder extends MessageToMessageEncoder<RpcWrapper> {
    

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcWrapper msg,
            List<Object> out) throws Exception {
        ByteBuf buffer = ctx.alloc().buffer(msg.rpcLength);
        
        msg.toBuffer(buffer);
        
        out.add(buffer);
    }
    
}
