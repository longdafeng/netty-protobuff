package us.longda.tools.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.StringUtil;

import java.util.List;

import org.apache.log4j.Logger;

/**
 * This file is almost copy from ByteToMessageDecoder
 * 
 * decode multiple ChannelRead's ByteBuf to one Message
 * 
 * ByteToMessageDecoder is devide one read to multiple Messages
 * 
 * @author zhongyan.feng
 * 
 */
public abstract class MessageDecoder extends ChannelInboundHandlerAdapter {
    private static final Logger LOG          = Logger.getLogger(MessageDecoder.class);
    
    protected ByteBuf           cumulation;
    protected boolean           singleDecode = true;
    protected boolean           decodeWasNull;
    
    /**
     * If {@code true} then only one message is decoded on each
     * {@link #channelRead(ChannelHandlerContext, Object)} call.
     * 
     * Default is {@code false} as this has performance impacts.
     */
    public boolean isSingleDecode() {
        return singleDecode;
    }
    
    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder. You usually do not need to rely on this value
     * to write a decoder. Use it only when you must use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer()
     * internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
        return internalBuffer().readableBytes();
    }
    
    /**
     * Returns the internal cumulative buffer of this decoder. You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ByteBuf internalBuffer() {
        if (cumulation != null) {
            return cumulation;
        } else {
            return Unpooled.EMPTY_BUFFER;
        }
    }
    
    protected boolean isContainBuffer() {
        return cumulation != null && cumulation.isReadable();
    }
    
    public void handleLeft(ChannelHandlerContext ctx) {
        if (isContainBuffer()) {
            LOG.warn("Connection has been inactive or removed, but internal buffer isn't empty");
            handleOneMessage(ctx);
            
        }
        
        if (cumulation != null) {
            cumulation.release();
            cumulation = null;
        }
    }
    
    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx)
            throws Exception {
        handleLeft(ctx);
        
        ctx.fireChannelReadComplete();
        handlerRemoved0(ctx);
    }
    
    /**
     * Gets called after the {@link ByteToMessageDecoder} was removed from the
     * actual context and it doesn't handle
     * events anymore.
     */
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
    }
    
    public boolean handleOneMessage(ChannelHandlerContext ctx)
            throws DecoderException {
        boolean isFinished = false;
        try {
            isFinished = checkFinish(cumulation);
            if (isFinished == true) {
                RecyclableArrayList out = RecyclableArrayList.newInstance();
                
                //callDecode(ctx, cumulation, out);
                
                decode(ctx, cumulation, out);
                
                for (int i = 0; i < out.size(); i++) {
                    ctx.fireChannelRead(out.get(i));
                }
                
                out.recycle();
                
                if (cumulation != null) {
                    cumulation.release();
                    cumulation = null;
                }
                
                return true;
            }
            
        } catch (DecoderException e) {
            LOG.error("Failed to decode message ", e);
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
        } finally {
            
        }
        
        return false;
    }
    
    public void handleReadBuf(ChannelHandlerContext ctx, ByteBuf data)
            throws Exception {
        if (cumulation == null) {
            cumulation = data;
        } else {
            
            if (cumulation.writerIndex() > cumulation.maxCapacity()
                    - data.readableBytes()) {
                ByteBuf oldCumulation = cumulation;
                // make the cumulation buffer bigger
                cumulation = ctx.alloc().buffer(
                        oldCumulation.readableBytes() + data.readableBytes()
                                * 8);
                cumulation.writeBytes(oldCumulation);
                oldCumulation.release();
            }
            cumulation.writeBytes(data);
            data.release();
        }
        
        boolean isHandled = handleOneMessage(ctx);
        if (isHandled == false) {
            decodeWasNull = true;
        } else {
            decodeWasNull = false;
        }
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        
        try {
            if (msg == null) {
                LOG.warn("Receive one empty buffer");
                decodeWasNull = true;
            } else if (msg instanceof ByteBuf) {
                handleReadBuf(ctx, (ByteBuf) msg);
            } else {
                LOG.warn("Receive object isn't ByteBuf:"
                        + msg.getClass().getName());
                
                RecyclableArrayList out = RecyclableArrayList.newInstance();
                
                out.add(msg);
                
                for (int i = 0; i < out.size(); i++) {
                    ctx.fireChannelRead(out.get(i));
                }
                
                out.recycle();
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Throwable t) {
            throw new DecoderException(t);
        } finally {
            
        }
    }
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (decodeWasNull) {
            decodeWasNull = false;
            if (!ctx.channel().config().isAutoRead()) {
                ctx.read();
            }
        } else if (isContainBuffer()) {
            handleOneMessage(ctx);
        }
        
        ctx.fireChannelReadComplete();
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        
        handleLeft(ctx);
        
        ctx.fireChannelInactive();
    }
    
    /**
     * Called once data should be decoded from the given {@link ByteBuf}. This
     * method will call {@link #decode(ChannelHandlerContext, ByteBuf, List)} as
     * long as decoding should take place.
     * 
     * @param ctx
     *            the {@link ChannelHandlerContext} which this
     *            {@link ByteToMessageDecoder} belongs to
     * @param in
     *            the {@link ByteBuf} from which to read data
     * @param out
     *            the {@link List} to which decoded messages should be added
     */
    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws DecoderException {
        try {
            while (in.isReadable()) {
                int outSize = out.size();
                int oldInputLength = in.readableBytes();
                decode(ctx, in, out);
                
                // Check if this handler was removed before try to continue the loop.
                // If it was removed it is not safe to continue to operate on the buffer
                //
                // See https://github.com/netty/netty/issues/1664
                if (ctx.isRemoved()) {
                    break;
                }
                
                if (outSize == out.size()) {
                    if (oldInputLength == in.readableBytes()) {
                        break;
                    } else {
                        continue;
                    }
                }
                
                if (oldInputLength == in.readableBytes()) {
                    throw new DecoderException(
                            StringUtil.simpleClassName(getClass())
                                    + ".decode() did not read anything but decoded a message.");
                }
                
                if (isSingleDecode()) {
                    break;
                }
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Throwable cause) {
            throw new DecoderException(cause);
        }
    }
    
    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be
     * called till either the input {@link ByteBuf} has nothing to read anymore,
     * till nothing was read from the input {@link ByteBuf} or till
     * this method returns {@code null}.
     * 
     * @param ctx
     *            the {@link ChannelHandlerContext} which this
     *            {@link ByteToMessageDecoder} belongs to
     * @param in
     *            the {@link ByteBuf} from which to read data
     * @param out
     *            the {@link List} to which decoded messages should be added
     * 
     * @throws Exception
     *             is thrown if an error accour
     */
    public abstract void decode(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws DecoderException;
    
    /**
     * Is called one last time when the {@link ChannelHandlerContext} goes
     * in-active. Which means the
     * {@link #channelInactive(ChannelHandlerContext)} was triggered.
     * 
     * By default this will just call
     * {@link #decode(ChannelHandlerContext, ByteBuf, List)} but sub-classes may
     * override this for some special cleanup operation.
     */
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws Exception {
        decode(ctx, in, out);
    }
    
    public abstract boolean checkFinish(ByteBuf buf) throws DecoderException;
}
