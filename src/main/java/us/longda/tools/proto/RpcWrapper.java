package us.longda.tools.proto;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import us.longda.tools.common.ByteConvert;

import com.google.protobuf.Message;

public class RpcWrapper {
    public static final byte[] RPC_HEADER = {'R', 'P', 'C', '\0'};
    public static final int    EXTRA_LEN = RPC_HEADER.length + 5 * 4;
    public static final int    MAX_RPC_SIZE = 2 * 1024 * 1024;
    
    public byte[] rpcTag = new byte[RPC_HEADER.length];
    public int    rpcLength; // whole rpc message length, including rpcTag and crc32
    public int    pbTypeNameLen;
    public byte[] pbTypeName;
    public int    pbDataLen;
    public byte[] pbData;
    public int    attachmentLen;
    public byte[] attachment;
    public int    crc32; // from rpcTag to attachment
    public Message pbMessage;
    
    public void parseFrom(ByteBuffer buf) throws Exception{
        buf.flip();
        
        buf.get(rpcTag);
        
        byte[] tmpBuf = new byte[4];
        
        buf.get(tmpBuf);
        rpcLength = ByteConvert.bytesToInt(tmpBuf);
        
        if (buf.limit() < rpcLength) {
            throw new Exception("buf.limit is less than rpcLength");
        }
        
        if (buf.hasArray()) {
            crc32 = ByteConvert.bytesToInt(buf.array(), rpcLength - 4);
            
            if (crc32 != 0) {
                CRC32 checksum = new CRC32();
                checksum.update(buf.array(), 0, rpcLength - 4);
                
                if (crc32 != (int)checksum.getValue()) {
                    throw new Exception( "CRC32 " + crc32 + " isn't equal checksum " + checksum.getValue());
                }
            }
        }
        
        buf.get(tmpBuf);
        pbTypeNameLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (pbTypeNameLen > 0) {
            pbTypeName = new byte[pbTypeNameLen];
            buf.get(pbTypeName);
        }
        
        buf.get(tmpBuf);
        pbDataLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (pbDataLen > 0) {
            pbData = new byte[pbDataLen];
            buf.get(pbData);
        }
        
        buf.get(tmpBuf);
        attachmentLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (attachmentLen > 0) {
            attachment = new byte[attachmentLen];
            buf.get(attachment);
        }

    }
    
    public void parseFrom(ByteBuf buf) throws Exception{
        buf.markReaderIndex();
        
        buf.readBytes(rpcTag);
        
        byte[] tmpBuf = new byte[4];
        
        buf.readBytes(tmpBuf);
        rpcLength = ByteConvert.bytesToInt(tmpBuf);
        
        if (buf.readableBytes() + 8 < rpcLength) {
            throw new Exception("buf.limit is less than rpcLength");
        }
        
        if (buf.hasArray()) {
        
            crc32 = ByteConvert.bytesToInt(buf.array(), rpcLength - 4);
            if (crc32 != 0) {
            
                CRC32 checksum = new CRC32();
                checksum.update(buf.array(), 0, rpcLength - 4);
                
                if (crc32 != (int)checksum.getValue()) {
                    throw new Exception( "CRC32 " + crc32 + " isn't equal checksum " + checksum.getValue());
                }
            }
        }
       
        
        buf.readBytes(tmpBuf);
        pbTypeNameLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (pbTypeNameLen > 0) {
            pbTypeName = new byte[pbTypeNameLen];
            buf.readBytes(pbTypeName);
        }
        
        buf.readBytes(tmpBuf);
        pbDataLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (pbDataLen > 0) {
            pbData = new byte[pbDataLen];
            buf.readBytes(pbData);
        }
        
        buf.readBytes(tmpBuf);
        attachmentLen = ByteConvert.bytesToInt(tmpBuf);
        
        if (attachmentLen > 0) {
            attachment = new byte[attachmentLen];
            buf.readBytes(attachment);
        }
        
        

    }
    
    
    public void toBuffer(ByteBuf buf) {
        buf.clear();
        
        buf.writeBytes(rpcTag);
        buf.writeBytes(ByteConvert.intToBytes(rpcLength));
        buf.writeBytes(ByteConvert.intToBytes(pbTypeNameLen));
        if (pbTypeNameLen > 0) {
            buf.writeBytes(pbTypeName, 0, pbTypeNameLen);
        }
        
        buf.writeBytes(ByteConvert.intToBytes(pbDataLen));
        if (pbDataLen > 0) {
            buf.writeBytes(pbData, 0, pbDataLen);
        }
        
        buf.writeBytes(ByteConvert.intToBytes(attachmentLen));
        if (attachmentLen > 0) {
            buf.writeBytes(attachment, 0, attachmentLen);
        }
        
        if (buf.hasArray()) {
            CRC32 checksum = new CRC32();
            checksum.update(buf.array(), 0, rpcLength - 4);
            crc32 = (int)checksum.getValue();
            
            buf.writeBytes(ByteConvert.intToBytes(crc32));
        }else {
            buf.writeBytes(ByteConvert.intToBytes(0));
        }
    }
}
