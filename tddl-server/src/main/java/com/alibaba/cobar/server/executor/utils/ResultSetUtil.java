package com.alibaba.cobar.server.executor.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.FieldPacket;
import com.alibaba.cobar.net.packet.MysqlResultSetPacket;
import com.alibaba.cobar.net.packet.ResultSetHeaderPacket;
import com.alibaba.cobar.net.packet.RowDataPacket;
import com.alibaba.cobar.net.util.CharsetUtil;
import com.alibaba.cobar.server.util.StringUtil;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.ResultSetMetaData;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;

/**
 * 将resultset转为二进制包
 * 
 * @author mengshi.sunmengshi 2013-11-19 下午4:27:55
 * @since 3.0.1
 */
public class ResultSetUtil {

    public static int toFlag(java.sql.ResultSetMetaData metaData, int column) throws SQLException {
        int flags = 0;
        if (metaData.isNullable(column) == 1) {
            flags |= 0001;
        }

        if (metaData.isSigned(column)) {
            flags |= 0020;
        }

        if (metaData.isAutoIncrement(column)) {
            flags |= 0200;
        }

        return flags;
    }

    // ResultSetMetaData.fields
    static java.lang.reflect.Field fieldsField = null;
    static java.lang.reflect.Field bufferField = null;
    static {
        try {
            fieldsField = ResultSetMetaData.class.getDeclaredField("fields");
            fieldsField.setAccessible(true);

            bufferField = Field.class.getDeclaredField("buffer");
            bufferField.setAccessible(true);
        } catch (SecurityException e) {
            throw new TddlNestableRuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static ByteBuffer resultSetToPacket(ResultSet rs, String charset, FrontendConnection c) throws Exception,
                                                                                                  UnsupportedEncodingException {

        MysqlResultSetPacket packet = new MysqlResultSetPacket();
        // 先执行一次next，因为存在lazy-init处理，可能写了packet head包出去，但实际获取数据时出错导致客户端出现lost
        // connection，没有任何其他异常
        boolean existNext = rs.next();

        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        boolean existUndecidedType = false;
        List<Integer> undecidedTypeIndexs = new ArrayList<Integer>();
        synchronized (packet) {
            if (packet.resulthead == null) {
                packet.resulthead = new ResultSetHeaderPacket();
                packet.resulthead.fieldCount = colunmCount;
            }
            int charsetIndex = CharsetUtil.getIndex(charset);
            if (colunmCount > 0) {
                if (packet.fieldPackets == null) {
                    packet.fieldPackets = new FieldPacket[colunmCount];
                    for (int i = 0; i < colunmCount; i++) {
                        int j = i + 1;
                        packet.fieldPackets[i] = new FieldPacket();
                        if (metaData instanceof com.mysql.jdbc.ResultSetMetaData) {
                            Field[] fields = (Field[]) fieldsField.get(metaData);
                            packet.fieldPackets[i] = new FieldPacket();
                            packet.fieldPackets[i].unpacked = (byte[]) bufferField.get(fields[i]);
                        } else {
                            packet.fieldPackets[i].catalog = StringUtil.encode("def", charset);
                            packet.fieldPackets[i].orgName = StringUtil.encode(metaData.getColumnName(j), charset);
                            packet.fieldPackets[i].name = StringUtil.encode(metaData.getColumnLabel(j), charset);
                            packet.fieldPackets[i].orgTable = StringUtil.encode(metaData.getTableName(j), charset);
                            packet.fieldPackets[i].table = StringUtil.encode(metaData.getTableName(j), charset);
                            packet.fieldPackets[i].db = StringUtil.encode(metaData.getSchemaName(j), charset);
                            packet.fieldPackets[i].length = 20;
                            packet.fieldPackets[i].flags = toFlag(metaData, j);
                            packet.fieldPackets[i].decimals = (byte) metaData.getScale(j);

                            packet.fieldPackets[i].charsetIndex = charsetIndex;

                            if (metaData.getColumnType(j) != DataType.UNDECIDED_SQL_TYPE) {
                                packet.fieldPackets[i].type = (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(metaData.getColumnType(j),
                                    packet.fieldPackets[i].decimals)) & 0xff);
                            } else {
                                packet.fieldPackets[i].type = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
                                undecidedTypeIndexs.add(i);
                                existUndecidedType = true;
                            }
                        }
                    }
                }
            }
        }

        ByteBuffer buffer = null;
        if (!existUndecidedType) {// 如果出现未决类型，等拿到第一条数据后再输出
            buffer = writeHeader(packet, c);
        }

        do {
            if (!existNext) {
                // 不存在记录，直接退出
                break;
            }
            RowDataPacket row = null;
            row = new RowDataPacket(colunmCount);
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                if (existUndecidedType && undecidedTypeIndexs.contains(i)) {
                    // 根据数据的类型，重新设置下type
                    DataType type = DataType.StringType;
                    try {
                        DataType objType = DataTypeUtil.getTypeOfObject(rs.getObject(j));
                        if (objType.getSqlType() != DataType.UNDECIDED_SQL_TYPE) {
                            type = objType;
                        }
                    } catch (Throwable e) {
                        // ignore
                        // 针对0000-00-00的时间类型可能getObject会失败，getBytes没问题
                    }

                    packet.fieldPackets[i].type = (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(type.getSqlType(),
                        packet.fieldPackets[i].decimals)) & 0xff);
                }

                if (packet.fieldPackets[i].type == MysqlDefs.FIELD_TYPE_BIT) {
                    row.fieldValues.add(rs.getBytes(j));
                } else {
                    byte[] bytes = rs.getBytes(j);
                    row.fieldValues.add(bytes == null ? null : bytes);
                }
            }

            if (existUndecidedType) {// 如果出现未决类型，一条数据都没有，强制输出packet
                buffer = writeHeader(packet, c);
                existUndecidedType = false;
            }

            // if (logger.isDebugEnabled()) {
            // logger.debug("fetch result row:" + row);
            // }
            // packet.addRowDataPacket(row);
            row.packetId = c.getNewPacketId();
            buffer = row.write(buffer, c);
        } while (rs.next());
        return buffer;
    }

    public static void eofToPacket(ByteBuffer buffer, FrontendConnection c) {
        // // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = c.getNewPacketId();
        buffer = lastEof.write(buffer, c);

        // write buffer
        c.write(buffer);
    }

    private static ByteBuffer writeHeader(MysqlResultSetPacket packet, FrontendConnection c) {
        // write header
        packet.resulthead.packetId = c.getNewPacketId();
        ByteBuffer buffer = packet.resulthead.write(c.allocate(), c);
        // write fields

        if (packet.fieldPackets != null) {
            for (FieldPacket field : packet.fieldPackets) {
                field.packetId = c.getNewPacketId();
                buffer = field.write(buffer, c);
            }

        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = c.getNewPacketId();
        buffer = eof.write(buffer, c);
        return buffer;
    }
}
