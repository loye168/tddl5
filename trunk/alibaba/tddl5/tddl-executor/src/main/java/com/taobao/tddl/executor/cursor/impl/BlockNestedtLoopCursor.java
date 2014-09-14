package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.IBlockNestedLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.IValueFilterCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * @author mengshi <mengshi.sunmengshi@taobao.com> Block Nested Loop Join
 */
public class BlockNestedtLoopCursor extends IndexNestedLoopMgetImpCursor implements IBlockNestedLoopCursor {

    ICursorFactory      cursorFactory    = null;
    ExecutionContext    executionContext = null;
    private final IJoin join;
    private RecordCodec leftCodec;

    public BlockNestedtLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                  List rightColumns, List columns, ICursorFactory cursorFactory, IJoin join,
                                  ExecutionContext executionContext, List leftRetColumns, List rightRetColumns)
                                                                                                               throws Exception{
        super(leftCursor,
            rightCursor,
            leftColumns,
            rightColumns,
            columns,
            leftRetColumns,
            rightRetColumns,
            join,
            executionContext);
        this.cursorFactory = cursorFactory;
        this.leftCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(ExecUtils.getColumnMetas(rightColumns));
        this.left_key = leftCodec.newEmptyRecord();
        this.executionContext = executionContext;

        this.join = join;
    }

    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRightByValueFilter(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                                throws TddlException {

        right_cursor.beforeFirst();
        if (isLeftOutJoin() && this.rightCursorMeta == null) {
            IRowSet kv = right_cursor.next();
            if (kv != null) {
                this.rightCursorMeta = kv.getParentCursorMeta();
            } else {
                rightCursorMeta = CursorMetaImp.buildNew(right_cursor.getReturnColumns());
            }

            right_cursor.beforeFirst();
        }

        IBooleanFilter ibf = ASTNodeFactory.getInstance().createBooleanFilter();
        ibf.setOperation(OPERATION.IN);
        ibf.setValues(new ArrayList<Object>());
        for (CloneableRecord record : leftJoinOnColumnCache) {
            Map<String, Object> recordMap = record.getMap();
            if (recordMap.size() == 1) {
                // 单字段in
                Entry<String, Object> entry = recordMap.entrySet().iterator().next();
                Object comp = entry.getValue();
                ibf.setColumn(this.rightJoinOnColumns.get(0).copy());
                ibf.getValues().add(comp);
            } else {
                // 多字段in
                if (ibf.getColumn() == null) {
                    ibf.setColumn(buildRowFunction(recordMap.keySet(), true, record));
                }

                ibf.getValues().add(buildRowFunction(recordMap.values(), false, record));
            }
        }

        IColumn rightColumn = (IColumn) this.rightJoinOnColumns.get(0);
        IValueFilterCursor vfc = this.cursorFactory.valueFilterCursor(executionContext, right_cursor, ibf);

        Map<CloneableRecord, DuplicateKVPair> records = new HashMap();
        if (isLeftOutJoin() && !isRightOutJoin()) {
            blockNestedLoopJoin(leftJoinOnColumnCache, rightColumn, vfc, records);
        } else if (!isLeftOutJoin() && !isRightOutJoin()) {
            // inner join
            blockNestedLoopJoin(leftJoinOnColumnCache, rightColumn, vfc, records);
        } else {
            throw new UnsupportedOperationException("leftOutJoin:" + isLeftOutJoin() + " ; rightOutJoin:"
                                                    + isRightOutJoin());
        }
        return records;
    }

    @Override
    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRight(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                   throws TddlException {
        // 子查询的话不能用mget
        // 因为子查询的话，join的列可以是函数，函数应该放在having里，而不是放在valueFilter里
        if (this.join.getRightNode().isSubQuery()) {
            return this.getRecordFromRightByValueFilter(leftJoinOnColumnCache);
        } else {
            return right_cursor.mgetWithDuplicate(leftJoinOnColumnCache, false, false);
        }
    }

    private void blockNestedLoopJoin(List<CloneableRecord> leftJoinOnColumnCache, IColumn rightColumn,
                                     IValueFilterCursor vfc, Map<CloneableRecord, DuplicateKVPair> records)
                                                                                                           throws TddlException {
        IRowSet kv = null;
        while ((kv = vfc.next()) != null) {

            kv = ExecUtils.fromIRowSetToArrayRowSet(kv);
            Object rightValue = ExecUtils.getValueByIColumn(kv, rightColumn);
            for (CloneableRecord record : leftJoinOnColumnCache) {
                Comparable comp = (Comparable) record.getMap().values().iterator().next();
                if (rightValue.equals(comp)) {
                    buildDuplicate(records, kv, record);
                    break;
                }
            }
        }
    }

    private void buildDuplicate(Map<CloneableRecord, DuplicateKVPair> records, IRowSet kv, CloneableRecord record) {
        DuplicateKVPair dkv = records.get(record);
        if (dkv == null) {
            dkv = new DuplicateKVPair(kv);
            records.put(record, dkv);
        } else {
            while (dkv.next != null) {
                dkv = dkv.next;
            }
            dkv.next = new DuplicateKVPair(kv);
        }
    }

    private IFunction buildRowFunction(Collection values, boolean isColumn, CloneableRecord record) {
        IFunction func = ASTNodeFactory.getInstance().createFunction();
        func.setFunctionName("ROW");
        StringBuilder columnName = new StringBuilder();
        columnName.append('(').append(StringUtils.join(values, ',')).append(')');
        func.setColumnName(columnName.toString());
        if (isColumn) {
            List<IColumn> columns = new ArrayList<IColumn>(values.size());
            for (Object value : values) {
                IColumn col = ASTNodeFactory.getInstance()
                    .createColumn()
                    .setColumnName((String) value)
                    .setDataType(record.getType((String) value));
                columns.add(col);
            }

            func.setArgs(columns);
        } else {
            func.setArgs(new ArrayList(values));
        }
        return func;
    }

}
