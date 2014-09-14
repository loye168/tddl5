package com.taobao.tddl.repo.demo.cursor;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.BlockNestedtLoopCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * @author mengshi <mengshi.sunmengshi@taobao.com> Block Nested Loop Join
 */
public class BlockNestedtLoopCursorDemo extends BlockNestedtLoopCursor {

    public BlockNestedtLoopCursorDemo(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                      List rightColumns, List columns, ICursorFactory cursorFactory, IJoin join,
                                      ExecutionContext executionContext, List leftRetColumns, List rightRetColumns)
                                                                                                                   throws Exception{
        super(leftCursor,
            rightCursor,
            leftColumns,
            rightColumns,
            columns,
            cursorFactory,
            join,
            executionContext,
            leftRetColumns,
            rightRetColumns);
    }

    @Override
    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRight(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                   throws TddlException {
        return this.getRecordFromRightByValueFilter(leftJoinOnColumnCache);

    }

}
