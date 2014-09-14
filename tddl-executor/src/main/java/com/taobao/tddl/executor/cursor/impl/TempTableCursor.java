package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.properties.ConnectionParams;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.common.TransactionConfig.Isolation;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ITempTableSortCursor;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.IndexType;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * 用于临时表排序，需要依赖bdb
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:01:15
 * @since 5.0.0
 */
public class TempTableCursor extends SortCursor implements ITempTableSortCursor {

    private static final String _TDDL_TABLENAME  = "_ANDOR_TABLENAME_";

    private final static Logger logger           = LoggerFactory.getLogger(TempTableCursor.class);

    protected static AtomicLong seed             = new AtomicLong(0);
    protected long              sizeProtection   = 100000;
    protected ICursorFactory    cursorFactory;
    boolean                     sortedDuplicates;

    private static final String identity         = "__identity__".toUpperCase();

    private boolean             inited           = false;

    private IRepository         repo;
    protected ISchematicCursor  tempTargetCursor = null;
    ITempTable                  tempTable        = null;

    protected ICursorMeta       returnMeta       = null;
    private final long          requestID;
    ExecutionContext            executionContext = null;
    private IQueryTree          plan             = null;
    /**
     * 是否阶段超出行数的数据
     */
    boolean                     cutRows          = false;

    /**
     * @param cursor
     * @param orderBys 按照何列排序
     * @param sortedDuplicates 是否允许重复
     * @param requestID
     * @param executionContext
     * @throws TddlException
     */
    public TempTableCursor(IRepository repo, ISchematicCursor cursor, List<IOrderBy> orderBys,
                           boolean sortedDuplicates, long requestID, ExecutionContext executionContext)
                                                                                                       throws TddlException,
                                                                                                       TddlException{
        super(cursor, orderBys);
        this.repo = repo;
        this.sortedDuplicates = sortedDuplicates;
        tempTargetCursor = cursor;
        this.requestID = requestID;
        this.executionContext = executionContext;

        sizeProtection = executionContext.getParamManager().getLong(ConnectionParams.TEMP_TABLE_MAX_ROWS);

        cutRows = executionContext.getParamManager().getBoolean(ConnectionParams.TEMP_TABLE_CUT_ROWS);
    }

    /**
     * @param cursor
     * @param sortedDuplicates
     * @param keyColumns
     * @param requestID
     * @param executionContext
     * @throws TddlException
     */
    public TempTableCursor(IRepository repo, ISchematicCursor cursor, boolean sortedDuplicates,
                           List<ColumnMeta> keyColumns, long requestID, ExecutionContext executionContext)
                                                                                                          throws TddlException{

        this(repo,
            cursor,
            ExecUtils.getOrderByFromColumnMetas(keyColumns),
            sortedDuplicates,
            requestID,
            executionContext);

    }

    public TempTableCursor(IRepository repo, ISchematicCursor cursor, boolean sortedDuplicates, long requestID,
                           ExecutionContext executionContext) throws TddlException{

        this(repo, cursor, ExecUtils.getOrderByFromColumnMetas(null), sortedDuplicates, requestID, executionContext);

    }

    public TempTableCursor(IRepository repo, IQueryTree plan, boolean sortedDuplicates, long requestID,
                           ExecutionContext executionContext) throws TddlException{

        this(repo, null, ExecUtils.getOrderByFromColumnMetas(null), sortedDuplicates, requestID, executionContext);

        this.plan = plan;

    }

    @Override
    public void init() throws TddlException {
        if (!inited) {

            prepare();
            inited = true;
        }

    }

    ITempTable createTempTable(String tableName) throws TddlException {
        ArrayList<ColumnMeta> keyColumns = new ArrayList<ColumnMeta>();
        ArrayList<ColumnMeta> valueColumns = new ArrayList<ColumnMeta>();
        // 用来生成CursorMeta的 列
        ArrayList<ColumnMeta> metaKeyColumns = new ArrayList<ColumnMeta>();
        ArrayList<ColumnMeta> metaValueColumns = new ArrayList<ColumnMeta>();
        // 遍历cursor的keyColumn，如果某个列是order by的条件，那么放到temp
        // cursor的key里面，否则放到value里面

        if (cursor == null) {
            if (plan == null) {
                throw new IllegalArgumentException("临时表即没给cursor，又没给执行计划");
            }

            cursor = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(plan, executionContext);
        }
        IRowSet rowSet = cursor.next();

        if (rowSet != null) {
            /**
             * 没有指定排序列，则以第一个列为主键
             */
            if (GeneralUtil.isEmpty(orderBys)) {
                List<ColumnMeta> orderByColumns = new ArrayList(0);
                orderByColumns.add(rowSet.getParentCursorMeta().getColumns().get(0));

                this.setOrderBy(ExecUtils.getOrderByFromColumnMetas(orderByColumns));
            }

            // 希望通过kv中的列来构造meta data，因为底层讲avg(pk)，解释成了count，和 sum
            buildColumnMeta(orderBys, keyColumns, valueColumns, rowSet, metaValueColumns, metaKeyColumns);
        } else {
            // 如果没值返回空 ，什么都不做
            return null;
        }

        // tableName = oldTableName;
        if (logger.isDebugEnabled()) {
            logger.debug("create temp table, tempTableName:\n" + tableName);
        }

        IndexMeta tempTableIndexMeta = new IndexMeta(tableName,
            keyColumns,
            valueColumns,
            IndexType.BTREE,
            Relationship.ONE_TO_ONE,
            true,
            true);

        TableMeta tmpSchema = new TableMeta(tableName, new ArrayList(), tempTableIndexMeta, null);

        tmpSchema.setTmp(true);
        tmpSchema.setSortedDuplicates(sortedDuplicates);
        // 增加临时表的判定
        ITempTable tempTable = repo.getTempTable(tmpSchema);

        CloneableRecord keyRecord = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(keyColumns)
            .newEmptyRecord();
        CloneableRecord valueRecord = null;
        if (!GeneralUtil.isEmpty(valueColumns)) {
            valueRecord = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(valueColumns).newEmptyRecord();
        }
        int i = 0;
        long size = 0;
        boolean protection = false;
        // 建立临时表，将老数据插入新表中。

        if (rowSet != null) {
            do {
                size++;
                if (size > sizeProtection) {
                    protection = true;
                    break;
                }
                for (ColumnMeta column : keyColumns) {
                    String colName = column.getName();
                    if (colName.contains(_TDDL_TABLENAME)) {
                        int m = colName.indexOf(_TDDL_TABLENAME);
                        colName = colName.substring(m + _TDDL_TABLENAME.length(), colName.length());

                    }
                    /**
                     * 在临时表的时候，来自不同2个表的相同的列，比如 a.id和b.id
                     * 会在这里被合并，导致后面取值有问题。现在准备将临时表中的columnName改成
                     * tableName.columnName的形式。顺序不变可以将后面的取值完成 有点hack..
                     */
                    Object o = ExecUtils.getValueByTableAndName(rowSet, column.getTableName(), colName, null);
                    keyRecord.put(column.getName(), o);
                }
                if (valueColumns != null && valueColumns.size() != 0) {
                    for (ColumnMeta valueColumn : valueColumns) {
                        String colName = valueColumn.getName();
                        if ("__IDENTITY__".equals(colName)) {
                            continue;
                        }
                        if (colName.contains(_TDDL_TABLENAME)) {
                            int m = colName.indexOf(_TDDL_TABLENAME);
                            colName = colName.substring(m + _TDDL_TABLENAME.length(), colName.length());
                        }
                        /**
                         * 在临时表的时候，来自不同2个表的相同的列，比如 a.id和b.id
                         * 会在这里被合并，导致后面取值有问题。现在准备将临时表中的columnName改成
                         * tableName.columnName的形式。顺序不变可以将后面的取值完成 有点hack..
                         */
                        Object o = ExecUtils.getValueByTableAndName(rowSet, valueColumn.getTableName(), colName, null);
                        valueRecord.put(valueColumn.getName(), o);
                    }
                }
                // value内加入唯一索引key。
                if (sortedDuplicates) {
                    valueRecord.put(identity, i++);
                }
                // System.out.println("TempTableSortCursor: "+key+"  "+value);
                tempTable.put(this.executionContext, keyRecord, valueRecord, tempTableIndexMeta, tableName);

            } while ((rowSet = cursor.next()) != null);
        }

        if (protection && !cutRows) {
            throw new ExecutorException("temp table size protection , check your sql or enlarge the limination size . ");
        }

        List<ColumnMeta> retColumns = new ArrayList<ColumnMeta>();
        retColumns.addAll(metaKeyColumns);
        retColumns.addAll(metaValueColumns);
        tempTable.setCursorMeta(CursorMetaImp.buildNew(retColumns));
        return tempTable;
    }

    protected void prepare() throws TddlException {
        final String tableName = "tmp." + "requestID." + requestID;
        ISchematicCursor ret = null;
        try {
            tempTable = executionContext.getTempTables().get(tableName, new Callable<ITempTable>() {

                @Override
                public ITempTable call() throws Exception {
                    return createTempTable(tableName);
                }
            });

            ExecutionContext tmpContext = new ExecutionContext();
            tmpContext.setIsolation(Isolation.READ_UNCOMMITTED);
            ret = tempTable.getCursor(tmpContext);
            List<TddlException> exs = new ArrayList();
            if (this.cursor != null) {
                exs = cursor.close(exs);
            }
            this.cursor = ret;
            if (!exs.isEmpty()) {
                throw GeneralUtil.mergeException(exs);
            }
        } catch (Exception ex) {
            List<TddlException> exs = new ArrayList();
            exs.add(new TddlException(ex));
            exs = this.close(exs);
            if (!exs.isEmpty()) {
                throw GeneralUtil.mergeException(exs);
            }
        }

        returnMeta = tempTable.getCursorMeta();
    }

    private void buildColumnMeta(List<IOrderBy> orderBys, List<ColumnMeta> keyColumns, List<ColumnMeta> valueColumns,
                                 IRowSet kv, List<ColumnMeta> metaValueColumns, List<ColumnMeta> metaKeyColumns) {
        ICursorMeta cursorMeta = kv.getParentCursorMeta();
        List<ColumnMeta> columnMeta = cursorMeta.getColumns();
        Set<IOrderBy> hashOrderBys = new HashSet<IOrderBy>();
        for (ColumnMeta cm : columnMeta) {
            /**
             * 为了防止有列名相同的列，新建一个列，列名由原表名和列名组成
             */
            ColumnMeta columnInTempTable = new ColumnMeta(cm.getTableName(),
                cm.getTableName() + _TDDL_TABLENAME + cm.getName(),
                cm.getDataType(),
                cm.getAlias(),
                cm.isNullable());
            IOrderBy orderBy = findOrderByInKey(orderBys, cm);
            if (orderBy != null) {
                keyColumns.add(columnInTempTable);
                hashOrderBys.add(orderBy);
                if (!metaKeyColumns.contains(cm)) {
                    metaKeyColumns.add(cm);
                }
            } else {
                // 列名与order by not match ,放到value里
                if (!valueColumns.contains(columnInTempTable)) {
                    valueColumns.add(columnInTempTable);
                }
                if (!metaValueColumns.contains(cm)) {
                    metaValueColumns.add(cm);
                }
            }
        }

        if (keyColumns.size() < orderBys.size()) {
            // order by 的列不存在与cursor中，不可能吧
            throw new RuntimeException("should not be here");

        }
        // 是否针对重复的value进行排序
        if (sortedDuplicates) {// identity
            valueColumns.add(new ColumnMeta(keyColumns.get(0).getTableName(),
                identity,
                DataType.IntegerType,
                null,
                true));
        }
    }

    private IOrderBy findOrderByInKey(List<IOrderBy> orderBys, ColumnMeta cm) {
        for (IOrderBy ob : orderBys) {
            ISelectable orderByColumn = ob.getColumn();
            String orderByTable = orderByColumn.getTableName();
            orderByTable = ExecUtils.getLogicTableName(orderByTable);
            if (cm != null && TStringUtil.equals(ExecUtils.getLogicTableName(cm.getTableName()), orderByTable)) {
                if (TStringUtil.equals(cm.getName(), orderByColumn.getColumnName())) {
                    return ob;
                }
            }
        }
        return null;
    }

    public ICursorFactory getCursorFactory() {
        return cursorFactory;
    }

    @Override
    public IRowSet next() throws TddlException {
        init();
        IRowSet next = parentCursorNext();
        next = wrap(next);
        // System.out.println("TempTableSortCursor: next "+next);
        return next;
    }

    private IRowSet wrap(IRowSet next) {
        if (next != null) {
            next = new RowSetWrapper(returnMeta, next);
        }
        return next;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        init();
        return parentCursorSkipTo(key);
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        init();
        return parentCursorSkipTo(key);
    }

    @Override
    public void beforeFirst() throws TddlException {
        init();
        parentCursorBeforeFirst();
    }

    @Override
    public IRowSet current() throws TddlException {
        init();
        IRowSet current = parentCursorCurrent();
        current = wrap(current);
        return current;
    }

    @Override
    public IRowSet first() throws TddlException {
        init();
        IRowSet first = parentCursorFirst();
        first = wrap(first);
        return first;
    }

    @Override
    public IRowSet last() throws TddlException {
        init();
        IRowSet last = parentCursorPrev();
        last = wrap(last);
        return last;
    }

    @Override
    public IRowSet prev() throws TddlException {
        init();
        IRowSet prev = parentCursorPrev();
        prev = wrap(prev);
        return prev;
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        exs = parentCursorClose(exs);

        return exs;
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

        sb.append(tabTittle).append("TempTableCursor").append("\n");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);
        if (this.cursor != null) {
            sb.append(this.tempTargetCursor.toStringWithInden(inden + 1));
        }
        return sb.toString();
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        init();
        return this.returnMeta.getColumns();
    }
}
