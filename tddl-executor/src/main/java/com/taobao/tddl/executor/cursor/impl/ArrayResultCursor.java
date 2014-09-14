package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IColumn;

public class ArrayResultCursor extends ResultCursor {

    private List<IRowSet>     rows    = new ArrayList();
    private Iterator<IRowSet> iter    = null;
    private ICursorMeta       meta;
    private final String      tableName;
    private List<ColumnMeta>  columns = new ArrayList();
    private IRowSet           current;
    private boolean           closed  = false;

    public ArrayResultCursor(String tableName, ExecutionContext context){
        super(context);
        this.tableName = tableName;
        this.originalSelectColumns = new ArrayList();
    }

    public void addColumn(String columnName, DataType type) {
        ColumnMeta c = new ColumnMeta(this.tableName, columnName, type, null, true, false);
        columns.add(c);

        IColumn ic = ASTNodeFactory.getInstance().createColumn();
        ic.setTableName(this.tableName);
        ic.setColumnName(columnName);
        ic.setDataType(type);
        originalSelectColumns.add(ic);
    }

    public void addRow(Object[] values) {
        ArrayRowSet row = new ArrayRowSet(this.meta, values);
        rows.add(row);
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        return false;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        return false;
    }

    @Override
    public IRowSet current() throws TddlException {
        return this.current;
    }

    @Override
    public IRowSet next() throws TddlException {
        if (iter == null) {
            iter = rows.iterator();
        }
        if (iter.hasNext()) {
            current = iter.next();
            return current;
        }
        current = null;
        return null;
    }

    @Override
    public IRowSet prev() throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IRowSet first() throws TddlException {
        if (rows.isEmpty()) return null;
        return rows.get(0);
    }

    @Override
    public void beforeFirst() throws TddlException {
        iter = rows.iterator();
        current = null;

    }

    @Override
    public IRowSet last() throws TddlException {
        if (rows.isEmpty()) return null;

        return rows.get(rows.size() - 1);
    }

    @Override
    public List<TddlException> close(List<TddlException> exceptions) {
        this.closed = true;
        if (exceptions == null) exceptions = new ArrayList();
        return exceptions;
    }

    @Override
    public boolean delete() throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        return columns;
    }

    public void initMeta() {
        this.meta = CursorMetaImp.buildNew(columns);
        // this.meta.setIsSureLogicalIndexEqualActualIndex(true);
    }

    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        sb.append("result : ").append("\n");
        sb.append(this.rows);
        return sb.toString();
    }
}
