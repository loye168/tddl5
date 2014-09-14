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
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.hbase.model.HbData;
import com.taobao.tddl.repo.hbase.operator.HbOperate;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-8-14 03:49:02
 */
public class HbCursor implements Cursor {

    private final IndexMeta               indexMeta;
    private final ICursorMeta             cursorMeta;
    private final Map<String, ColumnMeta> columnMap;
    private final Map<String, Integer>    columnIndex;
    private final HbOperate               hbOperate;
    private final AtomicBoolean           closed  = new AtomicBoolean(false);
    private final TablePhysicalSchema     physicalSchema;
    private final TableMeta               schema;
    private ResultScanner                 scanner;                           // scan句柄
    private IRowSet                       currentRowSet;                     // 当前rowSet
    private final IQuery                  query;
    private final AtomicBoolean           doQuery = new AtomicBoolean(false);

    public HbCursor(IndexMeta indexMeta, HbOperate hbOperate, TablePhysicalSchema physicalSchema, TableMeta schema,
                    IQuery query){
        this.indexMeta = indexMeta;
        this.cursorMeta = ExecUtils.convertToICursorMeta(this.indexMeta);
        this.columnMap = new HashMap<String, ColumnMeta>();
        this.columnIndex = new HashMap<String, Integer>();
        for (ColumnMeta cm : cursorMeta.getColumns()) {
            columnMap.put(cm.getName(), cm);
            Integer index = cursorMeta.getIndex(indexMeta.getTableName(), cm.getName(), cm.getAlias());
            columnIndex.put(cm.getName(), index);
        }

        this.hbOperate = hbOperate;
        this.physicalSchema = physicalSchema;
        this.schema = schema;
        this.query = query;

    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        HbData opData = new HbData();
        Map<String, Object> rowKeyColumnValues = new HashMap();
        for (String rowKeyColumn : this.physicalSchema.getRowKey()) {
            rowKeyColumnValues.put(rowKeyColumn, key.get(rowKeyColumn));
        }
        opData.setStartRow(this.physicalSchema.getRowKeyGenerator().encodeRowKey(rowKeyColumnValues));
        opData.setTableName(query.getTableName());
        ResultScanner scanner = hbOperate.scan(opData);

        if (this.scanner != null) {
            // 关闭一下老的scanner
            this.scanner.close();
        }
        this.scanner = scanner;
        return true;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public IRowSet current() throws TddlException {
        return currentRowSet;
    }

    @Override
    public IRowSet next() throws TddlException {
        if (doQuery.compareAndSet(false, true)) {
            HbData opData = new HbData();
            opData.setTableName(query.getTableName());
            this.scanner = hbOperate.scan(opData);
        }

        Result result;
        try {
            result = scanner.next();
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }
        if (result == null) {
            scanner.close();
            return null;
        }

        Map<Integer, Object> rowObjs = HbResultConvertor.convertResultToRow(result,
            columnMap,
            columnIndex,
            physicalSchema);
        IRowSet rowSet = new MapRowSet(cursorMeta, rowObjs);
        return rowSet;
    }

    @Override
    public IRowSet prev() throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public IRowSet first() throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public void beforeFirst() throws TddlException {
        throw new UnsupportedOperationException("not support HbCursor.beforeRirst() method!");
    }

    @Override
    public IRowSet last() throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (exs == null) {
            exs = new ArrayList();
        }
        // if checkRsIsNull() passed,concurrently close the cursor,will throw
        // NPE maybe.
        this.closed.compareAndSet(false, true);
        this.currentRowSet = null;

        if (this.scanner != null) this.scanner.close();
        return exs;
    }

    @Override
    public boolean delete() throws TddlException {
        throw new UnsupportedOperationException("not support HbCursor.delete()");
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        throw new UnsupportedOperationException("not support HbCursor.getNextDup()");
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        throw new UnsupportedOperationException("not support HbCursor.put(CloneableRecord key, CloneableRecord value)");
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
            Map<String, Object> rowKeyColumnValues = new HashMap();
            for (String rowKeyColumn : this.physicalSchema.getRowKey()) {
                rowKeyColumnValues.put(rowKeyColumn, key.get(rowKeyColumn));
            }

            HbData opData = new HbData();
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

    @Override
    public String toStringWithInden(int inden) {
        throw new UnsupportedOperationException("not support HbCursor.toStringWithInden(int inden)");
    }

    public ICursorMeta getCursorMeta() {
        return cursorMeta;
    }

    public HbOperate getHbOperate() {
        return hbOperate;
    }

    public TablePhysicalSchema getPhysicalSchemaMap() {
        return physicalSchema;
    }

    public IndexMeta getIndexMeta() {
        return indexMeta;
    }

    public TableMeta getSchema() {
        return schema;
    }

    public Map<String, ColumnMeta> getColumnMap() {
        return columnMap;
    }

    public Map<String, Integer> getColumnIndex() {
        return columnIndex;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        return null;
    }

    public IQuery getQuery() {
        return this.query;
    }
}
