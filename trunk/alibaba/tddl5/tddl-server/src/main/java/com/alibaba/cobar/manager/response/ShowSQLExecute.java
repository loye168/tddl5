package com.alibaba.cobar.manager.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.alibaba.cobar.Fields;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.util.LongUtil;
import com.alibaba.cobar.server.util.PacketUtil;
import com.alibaba.cobar.server.util.StringUtil;

/**
 * 查询各SQL在所有pool中的执行情况
 * 
 * @author wenfeng.cenwf 2011-4-20
 */
public final class ShowSQLExecute {

    private static final NumberFormat          nf          = DecimalFormat.getInstance();
    private static final int                   FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[]         fields      = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket             eof         = new EOFPacket();
    static {
        nf.setMaximumFractionDigits(3);

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQL_ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MIN_TIME", Fields.FIELD_TYPE_DOUBLE);
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
        for (int i = 0; i < 3; i++) {
            RowDataPacket row = getRow(1000 * (i + 1), c.getCharset());
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

    private static RowDataPacket getRow(long id, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(id));
        row.add(LongUtil.toBytes(100L));
        row.add(StringUtil.encode(nf.format(898.9), charset));
        row.add(StringUtil.encode(nf.format(8.8), charset));
        row.add(StringUtil.encode(nf.format(1.0), charset));
        return row;
    }

}
