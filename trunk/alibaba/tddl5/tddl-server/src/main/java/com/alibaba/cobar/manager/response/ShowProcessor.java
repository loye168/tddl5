package com.alibaba.cobar.manager.response;

import java.nio.ByteBuffer;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.Fields;
import com.alibaba.cobar.manager.ManagerConnection;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.server.util.IntegerUtil;
import com.alibaba.cobar.server.util.LongUtil;
import com.alibaba.cobar.server.util.PacketUtil;

/**
 * 查看处理器状态
 * 
 * @author xianmao.hexm 2010-1-25 下午01:11:00
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowProcessor {

    private static final int                   FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket header      = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[]         fields      = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket             eof         = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
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

    private static RowDataPacket getRow(NIOProcessor processor, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(processor.getName().getBytes());
        row.add(LongUtil.toBytes(processor.getNetInBytes()));
        row.add(LongUtil.toBytes(processor.getNetOutBytes()));
        row.add(LongUtil.toBytes(processor.getReactCount()));
        row.add(IntegerUtil.toBytes(processor.getRegisterQueueSize()));
        row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
        row.add(IntegerUtil.toBytes(processor.getBufferPool().size()));
        row.add(IntegerUtil.toBytes(processor.getBufferPool().capacity()));
        row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
        row.add(IntegerUtil.toBytes(processor.getBackends().size()));
        return row;
    }

}
