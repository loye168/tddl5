package com.taobao.tddl.repo.mysql.handler;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:28:28
 * @since 5.0.0
 */
public class UpdateMyHandler extends PutMyHandlerCommon {

    public UpdateMyHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta,
                                          My_JdbcHandler myJdbcHandler) throws TddlException {
        try {
            return myJdbcHandler.executeUpdate(executionContext, put, table, meta);
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }
}
