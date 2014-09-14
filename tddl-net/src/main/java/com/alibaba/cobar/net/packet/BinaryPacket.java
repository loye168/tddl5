package com.alibaba.cobar.net.packet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.util.BufferUtil;
import com.alibaba.cobar.net.util.StreamUtil;

/**
 * @author xianmao.hexm 2011-5-6 上午10:58:33
 */
public class BinaryPacket extends MySQLPacket {

    public static final byte OK           = 1;
    public static final byte ERROR        = 2;
    public static final byte HEADER       = 3;
    public static final byte FIELD        = 4;
    public static final byte FIELD_EOF    = 5;
    public static final byte ROW          = 6;
    public static final byte PACKET_EOF   = 7;
    public static final byte LOCAL_INFILE = -5;
    public byte[]            data;

    public void read(InputStream in) throws IOException {
        packetLength = StreamUtil.readUB3(in);
        packetId = StreamUtil.read(in);
        byte[] ab = new byte[packetLength];
        StreamUtil.read(in, ab, 0, ab.length);
        data = ab;
    }

    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c) {
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize());
        BufferUtil.writeUB3(buffer, getPacketLength());
        buffer.put(packetId);
        buffer = c.writeToBuffer(data, buffer);
        return buffer;
    }

    private int getPacketLength() {
        return data == null ? 0 : data.length;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Binary Packet";
    }

}
