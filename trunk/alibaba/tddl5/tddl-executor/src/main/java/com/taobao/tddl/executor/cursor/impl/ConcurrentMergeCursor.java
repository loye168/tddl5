package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.IMergeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * @author mengshi.sunmengshi 2014年3月21日 下午2:30:00
 * @since 5.0.4
 */
public class ConcurrentMergeCursor extends SchematicCursor implements IMergeCursor {

    protected List<ISchematicCursor> cursors      = new ArrayList(); ;
    protected final ExecutionContext executionContext;
    protected int                    currentIndex = 0;
    private List<IDataNodeExecutor>  subNodes;

    public ConcurrentMergeCursor(List<IDataNodeExecutor> subNodes, ExecutionContext executionContext){
        super(null, null, null);
        this.subNodes = subNodes;
        this.executionContext = executionContext;
    }

    @Override
    public void init() throws TddlException {
        if (this.inited) {
            return;
        }

        List<Future<ISchematicCursor>> futureCursors = new LinkedList<Future<ISchematicCursor>>();

        for (IDataNodeExecutor q : subNodes) {
            futureCursors.add(ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNodeFuture(q, executionContext));
        }

        List<TddlException> exs = new ArrayList();

        for (Future<ISchematicCursor> future : futureCursors) {
            try {
                cursors.add(future.get(15, TimeUnit.MINUTES));
            } catch (Throwable e) {
                exs.add(new TddlException(e));
            }
        }

        if (GeneralUtil.isNotEmpty(exs)) {
            throw GeneralUtil.mergeException(exs);
        }

        currentIndex = 0;
        super.init();
    }

    @Override
    public IRowSet next() throws TddlException {
        init();

        IRowSet iRowSet = innerNext();
        return iRowSet;
    }

    private IRowSet innerNext() throws TddlException {
        init();
        IRowSet ret;
        while (true) {
            if (currentIndex >= cursors.size()) {// 取尽所有cursor.
                return null;
            }
            ISchematicCursor isc = cursors.get(currentIndex);
            ret = isc.next();
            if (ret != null) {
                return ret;
            }

            switchCursor();
        }
    }

    private void switchCursor() {

        cursors.get(currentIndex).close(exceptionsWhenCloseSubCursor);

        currentIndex++;

    }

    List<TddlException> exceptionsWhenCloseSubCursor = new ArrayList();
    private IRowSet     current;

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        exs.addAll(exceptionsWhenCloseSubCursor);
        for (ISchematicCursor cursor : cursors) {
            exs = cursor.close(exs);
        }
        return exs;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        throw new UnsupportedOperationException("skip to is not supported");
    }

    @Override
    public List<ISchematicCursor> getSubCursors() {
        return cursors;
    }

    @Override
    public List<IOrderBy> getOrderBy() throws TddlException {
        init();
        return this.cursors.get(0).getOrderBy();
    }

    @Override
    public List<List<IOrderBy>> getJoinOrderBys() throws TddlException {
        return Arrays.asList(this.getOrderBy());

    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IRowSet current() throws TddlException {
        return this.current;
    }

    @Override
    public IRowSet prev() throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IRowSet first() throws TddlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beforeFirst() throws TddlException {
        this.currentIndex = 0;
        this.current = null;

        for (int i = 0; i < cursors.size(); i++) {
            cursors.get(i).beforeFirst();
        }

    }

    @Override
    public IRowSet last() throws TddlException {
        throw new UnsupportedOperationException();
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
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "ConcurrentMergeCursor ");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);

        for (ISchematicCursor cursor : cursors) {
            sb.append(cursor.toStringWithInden(inden + 1));
        }
        return sb.toString();

    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        init();

        return this.cursors.get(0).getReturnColumns();
    }

}
