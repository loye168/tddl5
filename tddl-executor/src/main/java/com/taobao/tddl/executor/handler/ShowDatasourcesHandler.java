package com.taobao.tddl.executor.handler;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.tddl.atom.TAtomDataSource;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.TopologyHandler;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class ShowDatasourcesHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(CostBasedOptimizer.class);

    public TAtomDataSource getAtomDatasource(DataSource s) {
        if (s instanceof TAtomDataSource) {
            return (TAtomDataSource) s;
        }

        if (s instanceof DataSourceWrapper) {
            return getAtomDatasource(((DataSourceWrapper) s).getWrappedDataSource());
        }

        throw new IllegalAccessError();
    }

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ArrayResultCursor result = new ArrayResultCursor("TRACE", executionContext);
        result.addColumn("ID", DataType.IntegerType);
        result.addColumn("SCHEMA", DataType.StringType);
        result.addColumn("NAME", DataType.StringType);

        result.addColumn("GROUP", DataType.StringType);
        result.addColumn("URL", DataType.StringType);
        result.addColumn("USER", DataType.StringType);
        result.addColumn("TYPE", DataType.StringType);
        result.addColumn("INIT", DataType.StringType);
        result.addColumn("MIN", DataType.StringType);

        result.addColumn("MAX", DataType.StringType);
        result.addColumn("IDLE_TIMEOUT", DataType.StringType);
        result.addColumn("MAX_WAIT", DataType.StringType);

        result.addColumn("ACTIVE_COUNT", DataType.StringType);
        result.addColumn("POOLING_COUNT", DataType.StringType);

        result.initMeta();
        int index = 0;

        Matrix matrix = ExecutorContext.getContext().getTopologyHandler().getMatrix();
        TopologyHandler topology = ExecutorContext.getContext().getTopologyHandler();

        for (Group group : matrix.getGroups()) {
            IGroupExecutor groupExecutor = topology.get(group.getName());

            Object o = groupExecutor.getRemotingExecutableObject();

            if (o != null && o instanceof TGroupDataSource) {
                TGroupDataSource ds = (TGroupDataSource) o;

                for (DataSource atom : ds.getDataSourceMap().values()) {
                    TAtomDataSource atomDs = this.getAtomDatasource(atom);
                    DruidDataSource d;
                    try {

                        if (!(atomDs.getDataSource().getTargetDataSource() instanceof DruidDataSource)) {
                            logger.warn("atom datasource is not druid?"
                                        + atomDs.getDataSource().getTargetDataSource().getClass());
                            continue;
                        }
                        d = (DruidDataSource) atomDs.getDataSource().getTargetDataSource();
                        result.addRow(new Object[] { index++, topology.getAppName(), d.getName(), group.getName(),
                                d.getUrl(), d.getUsername(), d.getDbType(), d.getInitialSize(), d.getMinIdle(),
                                d.getMaxActive(), d.getTimeBetweenEvictionRunsMillis() / (1000 * 60), d.getMaxWait(),
                                d.getActiveCount(), d.getPoolingCount() });

                    } catch (SQLException e) {
                        logger.error("", e);
                    }

                }
            }
        }

        return result;

    }
}
