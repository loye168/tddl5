package com.taobao.tddl.repo.demo.cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;

/**
 * @author mengshi.sunmengshi 2014年4月10日 下午5:17:00
 * @since 5.1.0
 */
public class DemoCursor implements Cursor {

    private final NavigableMap<Object, CloneableRecord> ccmap;
    private boolean                                     asc                   = true;
    private Iterator<Entry<Object, CloneableRecord>>    currentCursorIterator = null;
    private IndexMeta                                   indexMeta;
    private ICursorMeta                                 iCursorMeta;
    private boolean                                     duplicate             = false;

    private volatile boolean                            closed                = false;

    public DemoCursor(IndexMeta indexMeta, NavigableMap<Object, CloneableRecord> ccmap){
        this.ccmap = ccmap;
        this.indexMeta = indexMeta;
        this.iCursorMeta = ExecUtils.convertToICursorMeta(this.indexMeta);
        List<ColumnMeta> cmArray = indexMeta.getKeyColumns();
        if (cmArray != null) {
            if (cmArray.size() != 1) {
                throw new IllegalStateException("只支持单值索引yet");
            }

        } else {
            throw new IllegalStateException("key array is null");
        }
    }

    /**
	 * 
	 */
    private IRowSet current = null;

    @Override
    public boolean skipTo(CloneableRecord key) {

        return skipTo(key, asc);
    }

    private void validClosed() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
    }

    protected boolean skipTo(CloneableRecord key, boolean asc) {
        validClosed();
        Object val = DemoUtil.getValueOfKey(key);
        IRowSet kvPair = null;
        if (asc) {
            currentCursorIterator = ccmap.tailMap(val).entrySet().iterator();
            kvPair = next();
        } else {
            currentCursorIterator = ccmap.headMap(val).entrySet().iterator();
            kvPair = prev();
        }
        return kvPair != null;
    }

    @Override
    public boolean skipTo(KVPair key) {
        return skipTo(key.getKey());
    }

    @Override
    public IRowSet current() {
        validClosed();
        if (current != null) {
            return current;
        } else {
            if (currentCursorIterator != null) {
                IRowSet pair = getNextKV();
                current = pair;
            }
        }
        return current;
    }

    public void setCurrent(IRowSet kvPair) {
        if (kvPair != null) {
            current = kvPair;
        }

    }

    private IRowSet buildPair(Entry<Object, CloneableRecord> curEntry) {
        CloneableRecord record = curEntry.getValue();

        Object key = curEntry.getKey();
        List values = new ArrayList();
        values.add(key);
        values.addAll(record.getValueList());
        ArrayRowSet rowSet = new ArrayRowSet(iCursorMeta, values.toArray());
        return rowSet;
    }

    @Override
    public IRowSet next() {
        validClosed();
        if (currentCursorIterator == null || !asc) {
            currentCursorIterator = ccmap.entrySet().iterator();
            asc = true;
        }
        if (duplicate) {
            duplicate = false;
            return current;
        }
        IRowSet pair = getNextKV();
        return pair;
    }

    private IRowSet getNextKV() {
        Entry<Object, CloneableRecord> nextKV = null;
        if (currentCursorIterator.hasNext()) {
            nextKV = currentCursorIterator.next();
        }
        if (nextKV == null) {
            return null;
        }
        IRowSet pair = buildPair(nextKV);
        setCurrent(pair);
        return pair;
    }

    @Override
    public IRowSet prev() {
        validClosed();
        if (currentCursorIterator == null || asc) {
            currentCursorIterator = ccmap.descendingMap().entrySet().iterator();
            asc = false;
        }
        IRowSet pair = getNextKV();
        setCurrent(pair);
        return pair;
    }

    @Override
    public IRowSet first() {
        validClosed();
        currentCursorIterator = ccmap.entrySet().iterator();
        asc = true;
        return next();
    }

    @Override
    public IRowSet last() {
        validClosed();
        currentCursorIterator = ccmap.descendingMap().entrySet().iterator();
        asc = false;
        return prev();
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (exs == null) exs = new ArrayList();
        closed = true;
        return exs;
    }

    @Override
    public boolean delete() {
        validClosed();
        return false;
    }

    @Override
    public IRowSet getNextDup() {
        validClosed();

        throw new UnsupportedOperationException();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) {

        Map<CloneableRecord, DuplicateKVPair> res = new HashMap();
        for (CloneableRecord key : keys) {
            Object keyValue = DemoUtil.getValueOfKey(key);
            CloneableRecord valueRecord = this.ccmap.get(keyValue);

            if (valueRecord == null) {
                continue;
            }

            List values = new ArrayList();
            values.add(keyValue);
            values.addAll(valueRecord.getValueList());
            ArrayRowSet rowSet = new ArrayRowSet(iCursorMeta, values.toArray());
            DuplicateKVPair dup = new DuplicateKVPair(rowSet);

            res.put(key, dup);
        }

        return res;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) {
        return new ArrayList(this.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter).values());
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        sb.append("skiplist cursor : ");
        if (indexMeta != null) {
            sb.append(indexMeta.toStringWithInden(inden + 1));
        }
        return sb.toString();
    }

    @Override
    public void beforeFirst() {
        validClosed();
        currentCursorIterator = ccmap.entrySet().iterator();
        asc = true;

    }

    public ICursorMeta getiCursorMeta() {
        return iCursorMeta;
    }

    public void setiCursorMeta(ICursorMeta iCursorMeta) {
        this.iCursorMeta = iCursorMeta;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() {
        List retColumns = new ArrayList();
        retColumns.addAll(this.indexMeta.getKeyColumns());
        retColumns.addAll(indexMeta.getValueColumns());

        return retColumns;

    }

    @Override
    public boolean isDone() {
        return true;
    }

}
