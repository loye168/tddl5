package com.taobao.tddl.executor.cursor.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.IValueFilterCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFilter;

/**
 * 用于做没法走索引的条件过滤
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:01:53
 * @since 5.0.0
 */
public class ValueFilterCursor extends SchematicCursor implements IValueFilterCursor {

    protected IFilter          filter;
    Pattern                    pattern;
    String                     tarCache;
    protected ExecutionContext executionContext;

    public ValueFilterCursor(ISchematicCursor cursor, IFilter filter, ExecutionContext executionContext)
                                                                                                        throws TddlException{

        super(cursor, cursor == null ? null : null, cursor == null ? null : cursor.getOrderBy());
        // 将filter中的函数用规则引擎里的，带实际
        this.filter = filter;
        this.executionContext = executionContext;

    }

    private DuplicateKVPair allow(IFilter f, DuplicateKVPair dkv) throws TddlException {
        if (f == null) {
            return dkv;
        }
        // 链表头，第一个allow的DKV放在这里，如果所有都不allow，那么这里为空
        DuplicateKVPair rootAllowDKV = null;
        // 链表尾，用于append新元素
        DuplicateKVPair rootAllowDKVTail = null;
        // 遍历用dkv
        DuplicateKVPair currentDKV = dkv;
        if (allowOneDKV(f, currentDKV)) {
            rootAllowDKV = currentDKV;
            rootAllowDKVTail = currentDKV;
        }
        while ((currentDKV = dkv.next) != null) {
            if (allowOneDKV(f, currentDKV)) {
                if (rootAllowDKV == null) {
                    // 如果这是第一个满足要求的DKV.设置tail和root
                    rootAllowDKV = currentDKV;
                    rootAllowDKVTail = currentDKV;
                } else {// 前面已经有满足要求的DVK了，设置tail.next并更新tail
                    rootAllowDKVTail.next = currentDKV;
                    rootAllowDKVTail = currentDKV;
                }
            }
        }
        if (rootAllowDKVTail != null && rootAllowDKVTail.next != null) {// 最后一个元素，可能有next，但next不满足要求
            rootAllowDKVTail.next = null;
        }
        return rootAllowDKV;
    }

    @Override
    public IRowSet next() throws TddlException {
        IRowSet kv = null;
        while ((kv = parentCursorNext()) != null) {
            if (allow(filter, kv)) {
                return kv;
            }
        }
        return null;
    }

    private boolean allowOneDKV(IFilter f, DuplicateKVPair dkv) throws TddlException {
        // 遍历链表，如果有notallow，就丢掉他。
        boolean ok = allow(f, dkv.currentKey);
        return ok;
    }

    @SuppressWarnings("unchecked")
    boolean allow(IFilter f, IRowSet iRowSet) throws TddlException {
        if (f == null) {
            return true;
        }

        Object result = ((ScalarFunction) f.getExtraFunction()).scalarCalucate(iRowSet, this.executionContext);
        if (DataType.BooleanType.convertFrom(result)) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        if (super.skipTo(key)) {
            IRowSet kv = parentCursorCurrent();
            if (kv != null && allow(filter, kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        return skipTo(key.getKey());
    }

    @Override
    public IRowSet first() throws TddlException {
        IRowSet kv = parentCursorFirst();
        if (kv != null) {
            do {
                if (kv != null && allow(filter, kv)) {
                    return kv;
                }
            } while ((kv = parentCursorNext()) != null);
        }
        return null;
    }

    @Override
    public IRowSet last() throws TddlException {
        IRowSet kv = parentCursorLast();
        do {
            if (kv != null && allow(filter, kv)) {
                return kv;
            }
        } while ((kv = parentCursorPrev()) != null);
        return null;
    }

    @Override
    public IRowSet prev() throws TddlException {
        IRowSet kv = parentCursorPrev();
        do {
            if (kv != null && allow(filter, kv)) {
                return kv;
            }
        } while ((kv = parentCursorPrev()) != null);
        return null;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        Map<CloneableRecord, DuplicateKVPair> map = super.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
        if (map == null) {
            return null;
        }
        Map<CloneableRecord, DuplicateKVPair> retMap = new HashMap<CloneableRecord, DuplicateKVPair>(map.size());
        Iterator<Entry<CloneableRecord, DuplicateKVPair>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<CloneableRecord, DuplicateKVPair> entry = iterator.next();
            DuplicateKVPair dkv = entry.getValue();
            dkv = allow(filter, dkv);
            if (dkv != null) {
                retMap.put(entry.getKey(), dkv);
            }
        }
        return retMap;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String subQueryTab = GeneralUtil.getTab(inden);
        sb.append(subQueryTab).append("【Value Filter Cursor : ").append("\n");
        sb.append(subQueryTab).append(filter).append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
