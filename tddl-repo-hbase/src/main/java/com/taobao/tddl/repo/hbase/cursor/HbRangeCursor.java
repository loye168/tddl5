package com.taobao.tddl.repo.hbase.cursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.RangeMaker;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.cursor.impl.RangeCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.hbase.model.HbData;
import com.taobao.tddl.repo.hbase.operator.HbOperate;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

public class HbRangeCursor extends RangeCursor {

    private final IndexMeta               indexMeta;
    private final ICursorMeta             cursorMeta;
    private final HbOperate               hbOperate;
    private final Map<String, ColumnMeta> columnMap;
    private final Map<String, Integer>    columnIndex;
    private TablePhysicalSchema           physicalSchema;
    private final TableMeta               schema;
    private ResultScanner                 scanner;
    private IQuery                        query;

    public HbRangeCursor(ISchematicCursor cursor, IFilter lf, ExecutionContext ec) throws TddlException{
        super(cursor, lf, ec);
        if (cursor instanceof SchematicCursor) {
            HbCursor hbCursor = (HbCursor) (((SchematicCursor) cursor).cursor);
            hbOperate = hbCursor.getHbOperate();
            cursorMeta = hbCursor.getCursorMeta();
            indexMeta = hbCursor.getIndexMeta();
            physicalSchema = hbCursor.getPhysicalSchemaMap();
            columnMap = hbCursor.getColumnMap();
            columnIndex = hbCursor.getColumnIndex();
            schema = hbCursor.getSchema();
            query = hbCursor.getQuery();
            // try {
            // List<Exception> exs = hbCursor.close(null);
            // if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
            //
            // } catch (Exception e) {
            // throw new RuntimeException(e);
            // }
        } else {
            throw new UnsupportedOperationException(cursor.getClass().toString());
        }
    }

    @Override
    protected RangeMaker.Range makeRange(IFilter lf, List<IOrderBy> orderBys) {
        return new RangeMakerForHBase(ec).makeRange(lf, orderBys);
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (scanner != null) {
            scanner.close();
        }
        if (exs == null) exs = new ArrayList();
        return exs;
    }

    private final AtomicBoolean doQuery = new AtomicBoolean(false);

    @Override
    public IRowSet next() throws TddlException {
        IRowSet max = to;
        IRowSet min = from;

        if (max.equals(min)) {
            if (doQuery.compareAndSet(false, true)) {
                return executeQueryNoneRange(max);
            } else {
                return null;
            }
        } else if (doQuery.compareAndSet(false, true)) {
            executeQueryRange(max, min);
        }

        if (scanner == null) {
            throw new RuntimeException("ResultScanner init failed!");
        }

        Result result;
        try {
            result = scanner.next();
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }

        if (result != null) {
            Map<Integer, Object> rowObjs = HbResultConvertor.convertResultToRow(result,
                columnMap,
                columnIndex,
                physicalSchema);
            MapRowSet rowSet = new MapRowSet(cursorMeta, rowObjs);
            return rowSet;
        } else {
            scanner.close(); // 没数据的时候关闭一下scanner
            return null;
        }
    }

    private IRowSet executeQueryNoneRange(IRowSet key) {

        byte[] rowKey = IRowSetToByteArray(key);
        HbData opData = new HbData();
        opData.setRowKey(rowKey);
        opData.setTableName(query.getTableName());
        Result result = hbOperate.get(opData);
        if (result.isEmpty()) {
            return null;
        }

        // TODO 处理null值的情况
        Map<Integer, Object> rowObjs = HbResultConvertor.convertResultToRow(result,
            columnMap,
            columnIndex,
            physicalSchema);
        IRowSet rowSet = new MapRowSet(cursorMeta, rowObjs);
        return rowSet;
    }

    private byte[] IRowSetToByteArray(IRowSet key) {
        Map<String, Object> rowKeyColumnsAndValues = new HashMap();
        for (ColumnMeta keyColumn : schema.getPrimaryKey()) {
            Integer index = key.getParentCursorMeta().getIndex(null, keyColumn.getName(), keyColumn.getAlias());

            rowKeyColumnsAndValues.put(keyColumn.getName(), key.getObject(index));
        }

        byte[] rowKey = physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnsAndValues);

        return rowKey;
    }

    private void executeQueryRange(IRowSet max, IRowSet min) {
        HbData hbData = new HbData();

        hbData.setStartRow(IRowSetToByteArray(min));
        hbData.setEndRow(IRowSetToByteArray(max));
        hbData.setTableName(query.getTableName());
        scanner = this.hbOperate.scan(hbData);
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public IRowSet first() throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public IRowSet last() throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public IRowSet prev() throws TddlException {
        return super.prev();
    }

    @Override
    public IRowSet current() throws TddlException {
        return super.current();
    }

    @Override
    public void beforeFirst() throws TddlException {
        throw new UnsupportedOperationException("not supported .");
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        return super.getNextDup();
    }

    @Override
    public Cursor getCursor() {
        return super.getCursor();
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        Map<CloneableRecord, DuplicateKVPair> kvMap = new HashMap<CloneableRecord, DuplicateKVPair>(keys.size());
        List<DuplicateKVPair> retList = mgetWithDuplicateList(keys, prefixMatch, keyFilterOrValueFilter);
        Iterator<CloneableRecord> keyIterator = keys.iterator();
        for (DuplicateKVPair duplicateKVPair : retList) {
            kvMap.put(keyIterator.next(), duplicateKVPair);
        }
        return kvMap;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        List<DuplicateKVPair> list = new ArrayList<DuplicateKVPair>(keys.size());
        List<HbData> hbDatas = new ArrayList<HbData>(keys.size());
        for (CloneableRecord key : keys) {
            HbData opData = new HbData();
            Map<String, Object> rowKeyColumnValues = new HashMap();
            for (String rowKeyColumn : this.physicalSchema.getRowKey()) {
                rowKeyColumnValues.put(rowKeyColumn, key.get(rowKeyColumn));
            }

            opData.setRowKey(this.physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnValues));
            opData.setTableName(query.getTableName());
            hbDatas.add(opData);
        }
        Result[] results = hbOperate.mget(hbDatas);
        int index = 0;
        for (CloneableRecord key : keys) {
            Result result = results[index];
            if (result != null) {
                Map<Integer, Object> rowObjs = HbResultConvertor.convertResultToRow(result,
                    columnMap,
                    columnIndex,
                    physicalSchema);
                IRowSet rowSet = new MapRowSet(cursorMeta, rowObjs);
                DuplicateKVPair dkvpair = new DuplicateKVPair(rowSet);
                list.add(dkvpair);
            } else {
                list.add(null);
            }

        }

        return list;
    }
}
