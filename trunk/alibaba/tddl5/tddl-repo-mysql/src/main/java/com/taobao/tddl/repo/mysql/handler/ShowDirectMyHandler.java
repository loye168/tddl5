package com.taobao.tddl.repo.mysql.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IShow;
import com.taobao.tddl.repo.mysql.spi.My_Cursor;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Repository;

/**
 * 返回desc table结果
 * 
 * @author jianghang 2014-5-13 下午11:03:35
 * @since 5.1.0
 */
public class ShowDirectMyHandler extends QueryMyHandler {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        IShow show = (IShow) executor;
        ISchematicCursor showCursor = doShow(show, executionContext);
        if (show.getWhereFilter() != null) {
            showCursor = executionContext.getCurrentRepository()
                .getCursorFactory()
                .valueFilterCursor(executionContext, showCursor, show.getWhereFilter());

        }
        return showCursor;
    }

    public ISchematicCursor doShow(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        My_JdbcHandler jdbcHandler = ((My_Repository) executionContext.getCurrentRepository()).getJdbcHandler(dsGetter,
            executor,
            executionContext);
        My_Cursor my_cursor = new My_Cursor(jdbcHandler, null, executor, executor.isStreaming());
        return my_cursor.getResultSet();
    }
}
