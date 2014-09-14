package com.taobao.tddl.repo.hbase.spi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.hbase.cursor.HbConstant;
import com.taobao.tddl.repo.hbase.cursor.HbCursor;
import com.taobao.tddl.repo.hbase.cursor.HbResultConvertor;
import com.taobao.tddl.repo.hbase.model.HbData;
import com.taobao.tddl.repo.hbase.model.HbData.HbColumn;
import com.taobao.tddl.repo.hbase.operator.HbOperate;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

/**
 * 将HBase中 {@linkplain HTable}适配为Andor中的Table
 * 
 * @author jianghang 2013-7-26 下午1:37:51
 * @since 3.0.1
 */
public class HbTable implements ITable {

    private final TableMeta           schema;
    private final HbOperate           hbOperate;
    private final TablePhysicalSchema physicalSchema;

    public HbTable(TableMeta schema, HbOperate operate, TablePhysicalSchema physicalSchema){
        this.schema = schema;
        this.hbOperate = operate;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public TableMeta getSchema() {
        return this.schema;
    }

    @Override
    public void put(ExecutionContext executionContext, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta,
                    String dbName) throws TddlException {
        Map<String, Object> cvs = value.getMap();
        List<HbData> opDatas = new ArrayList<HbData>();
        Map<String, Object> rowKeyColumnsAndValues = new HashMap();
        for (ColumnMeta keyColumn : schema.getPrimaryKey()) {
            rowKeyColumnsAndValues.put(keyColumn.getName(), key.get(keyColumn.getName()));
        }

        byte[] rowKey = physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnsAndValues);
        for (Map.Entry<String, Object> entry : cvs.entrySet()) {
            String[] familyAndColumn = physicalSchema.getRealColumn(entry.getKey()).split(HbConstant.CF_COL_SPLITOR);
            HbData opData = new HbData();
            // opData.setRowKey(HbResultConvertor.changeToBytes(key.get(HbConstant.ROWKEY),
            // schema.getColumn(HbConstant.ROWKEY)));
            opData.setRowKey(rowKey);
            opData.setTableName(dbName);
            ColumnMeta cm = schema.getColumn(entry.getKey());
            byte[] columnByteValue = null;

            columnByteValue = this.physicalSchema.getColumnCoders()
                .get(cm.getName())
                .encodeToBytes(cm.getDataType(), entry.getValue());

            opData.addColumn(new HbColumn(familyAndColumn[0], familyAndColumn[1], columnByteValue));
            opDatas.add(opData);
        }

        hbOperate.mput(opDatas);
    }

    @Override
    public void delete(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                                  throws TddlException {
        List<HbData> opDatas = new ArrayList<HbData>();
        Map<String, Object> cvs = key.getMap();

        Map<String, Object> rowKeyColumnValues = new HashMap();
        for (String rowKeyColumn : this.physicalSchema.getRowKey()) {
            rowKeyColumnValues.put(rowKeyColumn, key.get(rowKeyColumn));
        }

        byte[] rowKey = this.physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnValues);
        for (Map.Entry<String, Object> entry : cvs.entrySet()) {
            if (!this.physicalSchema.getRowKey().contains(entry.getKey())) {
                String[] familyAndCol = physicalSchema.getRealColumn(entry.getKey()).split(HbConstant.CF_COL_SPLITOR);
                HbData opData = new HbData();
                opData.setRowKey(rowKey);
                opData.setTableName(dbName);
                opData.addColumn(new HbColumn(familyAndCol[0], familyAndCol[1]));
                opDatas.add(opData);
            }
        }

        hbOperate.mdelete(opDatas);
    }

    @Override
    public void close() {

    }

    @Override
    public CloneableRecord get(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta,
                               String dbName) {
        Map<String, Object> rowKeyColumnValues = new HashMap();
        for (String rowKeyColumn : this.physicalSchema.getRowKey()) {
            rowKeyColumnValues.put(rowKeyColumn, key.get(rowKeyColumn));
        }
        HbData opData = new HbData();
        opData.setRowKey(this.physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnValues));
        opData.setTableName(dbName);
        Result result = hbOperate.get(opData);
        // 转化为结果
        return HbResultConvertor.convertResultToRow(result, indexMeta, physicalSchema);

    }

    public HbOperate getHbOperate() {
        return hbOperate;
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta meta, IQuery iQuery)
                                                                                                       throws TddlException {
        HbCursor hc = new HbCursor(meta, this.hbOperate, this.physicalSchema, this.schema, iQuery);
        return new SchematicCursor(hc, hc.getCursorMeta(), ExecUtils.getOrderBy(meta));
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexMeta, String indexMetaName)
                                                                                                                   throws TddlException {
        throw new UnsupportedOperationException();

    }
}
