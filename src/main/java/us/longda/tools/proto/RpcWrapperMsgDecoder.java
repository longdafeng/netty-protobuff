package us.longda.tools.proto;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.util.List;

import org.apache.log4j.Logger;

import us.longda.tools.common.ByteConvert;

/**
 * This file is almost copy from ByteToMessageDecoder
 * 
 * decode mutliple ChannelRead's ByteBuf to one Message
 * 
 * @author zhongyan.feng
 *
 */
public class RpcWrapperMsgDecoder extends MessageDecoder {
    private static final Logger LOG = Logger.getLogger(RpcWrapperMsgDecoder.class);

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
     * {@link ByteBuf} has nothing to read anymore, till nothing was read from the input {@link ByteBuf} or till
     * this method returns {@code null}.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added

     * @throws Exception    is thrown if an error accour
     */
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws DecoderException{
        RpcWrapper rpcWrapper = new RpcWrapper();
        
        try {
            rpcWrapper.parseFrom(in);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            throw new DecoderException(e);
        }
        
        out.add(rpcWrapper);
    }

    public boolean checkFinish(ByteBuf  buf) throws DecoderException {
        int bufLen = buf.readableBytes();
        
        if (bufLen < 8) {
            return false;
        }
        
        byte[] readBytes = new byte[8];
        for (int i = 0; i < readBytes.length; i++) {
            readBytes[i] = buf.getByte(i);
        }
        
        if (readBytes[0] != RpcWrapper.RPC_HEADER[0]
                || readBytes[1] != RpcWrapper.RPC_HEADER[1]
                || readBytes[2] != RpcWrapper.RPC_HEADER[2]
                || readBytes[3] != RpcWrapper.RPC_HEADER[3]) {
            LOG.error("Unknow Message without RPC header");
            throw new DecoderException("Unknow Message without RPC header");
        }
        
        int messageLength = ByteConvert.bytesToInt(readBytes, 4);
        
        if (messageLength <= bufLen) {
            return true;
        }else {
            return false;
        }
        
        
    }
}
