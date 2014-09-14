package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.properties.ConnectionParams;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.IIndexNestLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.NamedRecord;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.statistics.FetchFromLeftOperation;
import com.taobao.tddl.statistics.JoinNextOperation;
import com.taobao.tddl.statistics.QueryFromLeftOperation;
import com.taobao.tddl.statistics.SQLOperation;

/**
 * 批量到右边去取数据的index nested loop实现
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:55:29
 * @since 5.0.0
 */
public class IndexNestedLoopMgetImpCursor extends IndexNestLoopCursor implements IIndexNestLoopCursor {

    /**
     * left cursor ，会先取一批数据（sizeKeyLimination个），这是那一批数据的遍历器
     */
    Iterator<IRowSet>                     leftIterator                  = null;
    /**
     * 当前取出的kvPair
     */
    Map<CloneableRecord, DuplicateKVPair> rightPairs;
    /**
     * 如果有重复，那么会放在这里
     */
    DuplicateKVPair                       rightDuplicateCache;

    /**
     * 左cursor join on columns 的value的遍历器，这个值是从leftIterator里面，根据left join on
     * column ，取出来放到队列里的。
     */
    Iterator<CloneableRecord>             leftJoinOnColumnCacheIterator = null;
    KVPair                                rightPair                     = null;

    boolean                               isLeftJoin                    = false;
    boolean                               useProxyResult                = true;

    protected ICursorMeta                 rightCursorMeta               = null;

    /**
     * 开始执行的时间
     */
    private Long                          executeStartTime              = System.currentTimeMillis();

    /**
     * 超时时间
     */
    private Long                          timeout                       = 0L;
    /**
     * 已迭代次数
     */
    private Long                          interationTimes               = 0L;

    /**
     * 最大迭代次数
     */
    private Long                          maxInterationTimes            = 0L;

    /**
     * 一次匹配中，batch传递的数据个数
     */
    int                                   sizeKeyLimination             = 20;

    /**
     * 假定每个key都有25个不同的value
     */
    int                                   sizeRetLimination             = 5000;
    private ExecutionContext              executionContext;
    private IJoin                         join;

    public IndexNestedLoopMgetImpCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                        List rightColumns, List columns, List leftRetColumns, List rightRetColumns,
                                        IJoin join, ExecutionContext executionContext) throws TddlException{
        this(leftCursor,
            rightCursor,
            leftColumns,
            rightColumns,
            columns,
            false,
            leftRetColumns,
            rightRetColumns,
            join,
            executionContext);
    }

    public IndexNestedLoopMgetImpCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                        List rightColumns, List columns, boolean prefix, List leftRetColumns,
                                        List rightRetColumns, IJoin join, ExecutionContext executionContext)
                                                                                                            throws TddlException{
        super(leftCursor, rightCursor, leftColumns, rightColumns, columns, prefix, leftRetColumns, rightRetColumns);
        setLeftRightJoin(join);

        maxInterationTimes = executionContext.getParamManager()
            .getLong(ConnectionParams.MAX_INDEX_NESTED_LOOP_ITERATION_TIMES);

        timeout = executionContext.getParamManager().getLong(ConnectionParams.INDEX_NESTED_LOOP_TIME_OUT);

        sizeKeyLimination = executionContext.getParamManager()
            .getInt(ConnectionParams.COUNT_OF_KEY_TO_RIGHT_INDEX_NESTED_LOOP);

        sizeRetLimination = executionContext.getParamManager()
            .getInt(ConnectionParams.MAX_ROW_RETURN_FROM_RIGHT_INDEX_NESTED_LOOP);

        this.executionContext = executionContext;
        this.join = join;

    }

    @Override
    protected IRowSet proecessJoinOneWithNoneProfix(boolean forward) throws TddlException {
        //
        isLeftJoin = isLeftOutJoin() & !isRightOutJoin();
        while (true) {
            if (leftIterator == null) {// 以左值iterator，作为判断整个结果集合能不能next下去的关键判断。
                boolean hasMore = getMoreRecord(forward);
                if (!hasMore) {// 没有新结果集合，直接返回null
                    return null;
                }
            }
            IRowSet pair = match(leftIterator, leftJoinOnColumnCacheIterator, rightPairs);
            // pair.toString();
            if (pair != null) {
                return pair;
            } else {
                // 取尽，让这俩为空，这样下一次循环就可以从新去建心的iterator，或者没有iterator返回空了
                leftIterator = null;
                rightPairs = null;
                leftJoinOnColumnCacheIterator = null;
            }
        }
    }

    /**
     * <pre>
     * 这个方法的核心作用，就是把已经取出的左面一组数，和右面的一组数，按照join on
     * column的条件，从两边各找到一个对应的Row.然后把这两个row join到一起。 右列与左列排序相同，但右列可能出现几种情况： 
     * 1. 右列可能缺少左列中的某个值
     * 2. 右列也可能拥有多个与左列某个值相同的值（重复）
     * 若左列当前值为空，从左面拿一个值出来，再从右面拿一个值出来，做比较。否则使用左列当前值
     * 因为可能出现左列某值在右列为空的情况，为了简化场景（主要简化：左要知道右是否有左，需要遍历全结果集），所以以右作为驱动表。
     * 右的值，一定会在左中有对应的值。 找到他，组合成joinRecord.放到current里面。然后返回true即可。
     * </pre>
     * 
     * @param leftIterator 左值的一个遍历队列便利器
     * @param leftJoinOnColumnCacheIterator2 左值中，用来做join on column的数据的队列便利器
     * @param rightPairs2
     * 根据左面的数据id,从右面的结果集中取出的一组数据，这组数据内是可能有重复数据的。这个Map的key，是join on column中要求的数据
     * value，是拥有这行数据的KVPair的集合（也就是拥有相同join on column的数据的集合，是个链表)
     * @return 返回一个Join后的结果。
     * @throws TddlException
     */
    protected IRowSet match(Iterator<IRowSet> leftIterator, Iterator<CloneableRecord> leftJoinOnColumnCacheIterator2,
                            Map<CloneableRecord/* 相同的key */, DuplicateKVPair/* 见DuplicateKVPair注释 */> rightPairs2)
                                                                                                                  throws TddlException {
        IRowSet right = null;
        if (rightDuplicateCache == null) {
            while (leftIterator.hasNext()) {
                left = leftIterator.next();
                if (!leftJoinOnColumnCacheIterator2.hasNext()) {
                    throw new IllegalStateException("should not be here . leftJoinOnColumns is end, but left kvPair is not");
                }

                left_key = leftKeyNext(leftJoinOnColumnCacheIterator2);
                rightDuplicateCache = rightPairs2.get(left_key);
                if (rightDuplicateCache != null) {
                    // 匹配，找到了
                    right = rightDuplicateCache.currentKey;
                    current = joinRecord(left, right);

                    // 如果有重复，那么指针下移，让下次可以直接去选择。
                    rightDuplicateCache = rightDuplicateCache.next;
                    return current;
                } else if (isLeftJoin) {
                    // 如果是left join

                    try {
                        List<ColumnMeta> rightColumns = this.right_cursor.getReturnColumns();
                        List<ColumnMeta> leftColumns = this.left_cursor.getReturnColumns();

                        if (this.rightCursorMeta == null) {
                            // 都是按照返回列构建的，一致
                            this.buildSchemaFromReturnColumns(leftColumns, rightColumns);
                            this.rightCursorMeta = CursorMetaImp.buildNew(rightColumns);

                        } else {
                            buildSchemaInJoin(left.getParentCursorMeta(), rightCursorMeta);
                        }

                        // 建一个都为null的rouset
                        IRowSet rightRowSet = new ArrayRowSet(rightCursorMeta, new Object[rightCursorMeta.getColumns()
                            .size()]);
                        current = joinRecord(left, rightRowSet);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    return current;
                }
            }
            // 左值取尽
            return null;
        } else {
            /*
             * 如果 right节点的cache不为空，则证明某个右值还有数据相同的重复的Row. 所以左值不下移，右值取链表下一个。
             */
            right = rightDuplicateCache.currentKey;
            current = joinRecord(left, right);
            // 如果有重复，那么指针下移，让下次可以直接去选择。
            rightDuplicateCache = rightDuplicateCache.next;
            return current;
        }
    }

    private CloneableRecord leftKeyNext(Iterator<CloneableRecord> leftJoinOnColumnCacheIterator2) {
        CloneableRecord cr = leftJoinOnColumnCacheIterator2.next();
        return new NamedRecord(cr.getMap().keySet().iterator().next(), cr);
    }

    private boolean getMoreRecord(boolean forward) throws TddlException {

        if (maxInterationTimes > 0) {
            if (++interationTimes > maxInterationTimes) {
                return false;
            }
        }

        if (timeout > 0) {
            if (System.currentTimeMillis() - this.executeStartTime >= timeout) {
                return false;
            }
        }
        List<CloneableRecord> leftJoinOnColumnCache = new ArrayList<CloneableRecord>(sizeKeyLimination);
        List<IRowSet> leftRows = new ArrayList<IRowSet>(sizeKeyLimination);

        if (this.executionContext.isEnableTrace()) {
            QueryFromLeftOperation op = new QueryFromLeftOperation(this.join.getLeftNode().getAlias());
            executionContext.getTracer().trace(op);
        }

        boolean hasMore = fillCache(leftJoinOnColumnCache, leftRows, forward);

        if (!hasMore) {
            return false;
        }

        if (this.executionContext.isEnableTrace()) {
            SQLOperation op = new FetchFromLeftOperation(leftJoinOnColumnCache);
            executionContext.getTracer().trace(op);
        }

        // 如果使用index nest loop .那么右表一定是按主key进行查询的。
        rightPairs = getRecordFromRight(leftJoinOnColumnCache);
        leftIterator = leftRows.iterator();
        leftJoinOnColumnCacheIterator = leftJoinOnColumnCache.iterator();
        return true;
    }

    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRight(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                   throws TddlException {

        return right_cursor.mgetWithDuplicate(leftJoinOnColumnCache, false, true);
    }

    /**
     * 将left cursor 取出 sizeKeyLimination个。 放到缓存里
     * 
     * @param leftJoinOnColumnCache
     * @param leftKVPair
     * @return
     * @throws TddlException
     * @throws InterruptedException
     */
    private boolean fillCache(List<CloneableRecord> leftJoinOnColumnCache, List<IRowSet> leftKVPair, boolean forward)
                                                                                                                     throws TddlException {

        int currentSize = 0;
        boolean hasMore = false;
        while (getOneLeftCursor(forward) != null) {
            // 有一个，就算has
            hasMore = true;
            GeneralUtil.checkInterrupted();
            putLeftCursorValueIntoReturnVal();
            // 上面的方法用来找到left Join on columns ,然后放入key里面，这里就直接利用这个key,去右边查询
            leftJoinOnColumnCache.add(right_key);
            leftKVPair.add(left);
            currentSize++;
            if (sizeKeyLimination <= currentSize) {
                return true;
            }
        }
        // 用后，清空，其他地方还可能会用到这两个类变量
        left = null;
        left_key = null;
        // 耗尽
        return hasMore;
    }

    @Override
    public IRowSet next() throws TddlException {

        IRowSet row = super.next();

        if (row != null) {
            if (executionContext.isEnableTrace()) {
                JoinNextOperation op = new JoinNextOperation(ExecUtils.fromIRowSetToArrayRowSet(current));
                executionContext.getTracer().trace(op);
            }
        }

        return row;
    }

}
