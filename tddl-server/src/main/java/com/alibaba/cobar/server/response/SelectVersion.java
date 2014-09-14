package com.alibaba.cobar.server.response;

import java.nio.ByteBuffer;

import com.alibaba.cobar.Fields;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.util.PacketUtil;

/**
 * @author xianmao.hexm 2012-4-17
 */
public class SelectVersion {

    private static final int                   FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[]         fields      = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket             eof         = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("VERSION()", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(ServerConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = header.write(buffer, c);
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c);
        }
        buffer = eof.write(buffer, c);
        byte packetId = eof.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(FrontendConnection.getServerVersion().getBytes());
        row.packetId = ++packetId;
        buffer = row.write(buffer, c);
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);
        c.write(buffer);
    }

}
