package com.alibaba.cobar.manager.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.Fields;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.util.PacketUtil;

/**
 * @author xianmao.hexm 2010-9-30 下午01:47:38
 */
public final class ShowRouter {

    private static final int                   FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[]         fields      = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket             eof         = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("PROCESSOR_NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ROUTE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TIME_COUNT", Fields.FIELD_TYPE_FLOAT);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_ROUTE_TIME", Fields.FIELD_TYPE_FLOAT);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_ROUTE_SQL_ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c);
        }

        // write eof
        buffer = eof.write(buffer, c);

        // write rows
        byte packetId = eof.packetId;
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            RowDataPacket row = getRow(p, c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);

        // write buffer
        c.write(buffer);
    }

    private static final NumberFormat nf = DecimalFormat.getInstance();
    static {
        nf.setMaximumFractionDigits(3);
    }

    private static RowDataPacket getRow(NIOProcessor processor, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(processor.getName().getBytes());
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        return row;
    }

}
