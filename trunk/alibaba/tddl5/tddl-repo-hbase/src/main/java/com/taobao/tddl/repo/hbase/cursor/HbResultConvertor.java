package com.taobao.tddl.repo.hbase.cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.FixedLengthRecord;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-9-7上午10:05:13
 */
public class HbResultConvertor {

    public static final String ENCODING = "UTF-8";

    public static Map<Integer, Object> convertResultToRow(Result result, Map<String, ColumnMeta> columnMap,
                                                          Map<String, Integer> columnIndex,
                                                          TablePhysicalSchema physicalSchema) {
        KeyValue[] kvs = result.raw();
        Map<String, byte[]> rowMap = new HashMap<String, byte[]>();
        byte[] rowKey = result.getRow();
        Map<String, Object> rowKeyColumnValues = physicalSchema.getRowKeyGenerator().decodeRowKey(rowKey);

        for (KeyValue kv : kvs) {
            StringBuilder cfAndCol = new StringBuilder();
            cfAndCol.append(new String(kv.getFamily()));
            cfAndCol.append(HbConstant.CF_COL_SEP);
            cfAndCol.append(new String(kv.getQualifier()));
            rowMap.put(cfAndCol.toString(), kv.getValue());
        }

        // 应对hbase不固定的schema
        Map<Integer, Object> r = new TreeMap<Integer, Object>();
        for (Map.Entry<String, byte[]> cv : rowMap.entrySet()) {
            String innerColumn = physicalSchema.getInnerColumn(cv.getKey());

            // hbase中存在的列，但是map中没有配置，忽略
            if (innerColumn == null) continue;
            r.put(columnIndex.get(innerColumn), changeType(cv.getValue(), columnMap.get(innerColumn), physicalSchema));
        }

        for (String name : rowKeyColumnValues.keySet()) {
            r.put(columnIndex.get(name), rowKeyColumnValues.get(name));
        }
        return r;
    }

    public static CloneableRecord convertResultToRow(Result result, IndexMeta meta, TablePhysicalSchema physicalSchema) {
        KeyValue[] kvs = result.raw();
        Map<String, byte[]> rowMap = new HashMap<String, byte[]>();

        List<ColumnMeta> cms = new ArrayList(meta.getKeyColumns().size() + meta.getValueColumns().size());
        cms.addAll(meta.getKeyColumns());
        cms.addAll(meta.getValueColumns());

        Map<String, Object> rowKeyColumnValues = physicalSchema.getRowKeyGenerator().decodeRowKey(result.getRow());

        for (KeyValue kv : kvs) {
            StringBuilder cfAndCol = new StringBuilder();
            cfAndCol.append(new String(kv.getFamily()));
            cfAndCol.append(HbConstant.CF_COL_SEP);
            cfAndCol.append(new String(kv.getQualifier()));
            rowMap.put(cfAndCol.toString(), kv.getValue());
        }

        // 应对hbase不固定的schema
        Map<String, Object> r = new TreeMap<String, Object>();
        for (Map.Entry<String, byte[]> cv : rowMap.entrySet()) {
            String innerColumn = physicalSchema.getInnerColumn(cv.getKey());

            // hbase中存在的列，但是map中没有配置，忽略
            if (innerColumn == null) continue;
            r.put(innerColumn, changeType(cv.getValue(), meta.getColumnMeta(innerColumn), physicalSchema));
        }

        for (String name : rowKeyColumnValues.keySet()) {
            r.put(name, rowKeyColumnValues.get(name));
        }

        FixedLengthRecord record = new FixedLengthRecord(cms);

        record.putAll(r);

        return record;
    }

    private static Object changeType(byte[] value, ColumnMeta cm, TablePhysicalSchema schema) {
        return schema.getColumnCoders().get(cm.getName()).decodeFromBytes(cm.getDataType(), value);
    }

}
