package com.alibaba.cobar.net.packet;

import java.nio.ByteBuffer;

import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.util.BufferUtil;
import com.alibaba.cobar.net.util.MySQLMessage;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-16 上午10:45:01
 */
public class ErrorPacket extends MySQLPacket {

    public static final byte    FIELD_COUNT      = (byte) 0xff;
    private static final byte   SQLSTATE_MARKER  = (byte) '#';
    private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    public byte                 fieldCount       = FIELD_COUNT;
    public int                  errno;
    public byte                 mark             = SQLSTATE_MARKER;
    public byte[]               sqlState         = DEFAULT_SQLSTATE;
    public byte[]               message;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        fieldCount = mm.read();
        errno = mm.readUB2();
        if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        errno = mm.readUB2();
        if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }

    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c) {
        int size = getPacketLength();
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, errno);
        buffer.put(mark);
        buffer.put(sqlState);
        if (message != null) {
            buffer = c.writeToBuffer(message, buffer);
        }
        return buffer;
    }

    public void write(FrontendConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, getPacketLength());
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, errno);
        buffer.put(mark);
        buffer.put(sqlState);
        if (message != null) {
            buffer = c.writeToBuffer(message, buffer);
        }
        c.write(buffer);
    }

    private int getPacketLength() {
        int size = 9;// 1 + 2 + 1 + 5
        if (message != null) {
            size += message.length;
        }
        return size;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Error Packet";
    }

}
