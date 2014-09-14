package com.taobao.tddl.repo.demo.spi;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.IBlockNestedLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.CursorFactoryDefaultImpl;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.repo.demo.cursor.BlockNestedtLoopCursorDemo;

/**
 * @author mengshi.sunmengshi 2014年4月10日 下午5:16:54
 * @since 5.1.0
 */
public class CursorFactoryDemoImp extends CursorFactoryDefaultImpl {

    @Override
    public IBlockNestedLoopCursor blockNestedLoopJoinCursor(ExecutionContext executionContext,
                                                            ISchematicCursor left_cursor,
                                                            ISchematicCursor right_cursor, List left_columns,
                                                            List right_columns, List columns, IJoin join)
                                                                                                         throws TddlException {

        try {
            return new BlockNestedtLoopCursorDemo(left_cursor,
                right_cursor,
                left_columns,
                right_columns,
                columns,
                this,
                join,
                executionContext,
                join.getLeftNode().getColumns(),
                join.getRightNode().getColumns());
        } catch (Exception e) {
            closeParentCursor(left_cursor);
            closeParentCursor(right_cursor);
            throw new TddlException(e);
        }
    }
}
