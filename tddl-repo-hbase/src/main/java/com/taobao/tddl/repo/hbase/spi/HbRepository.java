package com.taobao.tddl.repo.hbase.spi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.repo.hbase.config.TablePhysicalInfoParser;
import com.taobao.tddl.repo.hbase.executor.HBaseGroupExecutor;
import com.taobao.tddl.repo.hbase.operator.HbFactory;
import com.taobao.tddl.repo.hbase.operator.HbFactory.HbaseConf;
import com.taobao.tddl.repo.hbase.operator.HbOperate;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-8-10 03:46:04
 */
public class HbRepository extends AbstractLifecycle implements IRepository {

    protected LoadingCache<String, LoadingCache<TableMeta, ITable>> tables;
    protected LoadingCache<Group, IGroupExecutor>                   executors;

    protected Map<String, HBaseGroupExecutor>                       groupNameAndExecutors = new ConcurrentHashMap<String, HBaseGroupExecutor>();
    private ICursorFactory                                          cursorFactory         = null;
    private ICommandHandlerFactory                                  commandFactory;

    private Map<String, TablePhysicalSchema>                        physicalSchema        = new HashMap<String, TablePhysicalSchema>();

    public HbRepository(String schemaData){ // 接收hbase和sql中逻辑表俄转化信息
        if (schemaData == null) {
            return;
        }

        InputStream sis = new ByteArrayInputStream(schemaData.getBytes());
        try {
            physicalSchema = TablePhysicalInfoParser.parseAll(sis);
        } finally {
            IOUtils.closeQuietly(sis);
        }
    }

    @Override
    public ITable getTable(final TableMeta meta, final String groupNode, String actualTableName) throws TddlException {
        if (meta.isTmp()) {
            return getTempTable(meta);
        } else {
            try {
                return tables.get(groupNode).get(meta);
            } catch (ExecutionException e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    @Override
    public boolean isWriteAble() {
        return true;
    }

    @Override
    public boolean isEnhanceExecutionModel(String groupKey) {
        return false;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        if (this.cursorFactory == null) {
            this.cursorFactory = new HbCursorFactory();
        }

        return this.cursorFactory;
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        if (this.commandFactory == null) {
            this.commandFactory = new HbCommandExecutorFactory();
        }

        return this.commandFactory;
    }

    private HbOperate getHBCluster(Map<String, String> config) throws TddlException {
        HbFactory factory = new HbFactory();
        factory.setClusterConfig(config);

        HbOperate operate = new HbOperate(factory);
        operate.init();

        return operate;
    }

    @Override
    public ITempTable getTempTable(TableMeta meta) throws TddlException {
        return null;
    }

    @Override
    public RepositoryConfig getRepoConfig() {
        return null;
    }

    @Override
    public IGroupExecutor getGroupExecutor(Group group) {
        try {
            return executors.get(group);
        } catch (ExecutionException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    public void doInit() {

        tables = CacheBuilder.newBuilder().build(new CacheLoader<String, LoadingCache<TableMeta, ITable>>() {

            @Override
            public LoadingCache<TableMeta, ITable> load(final String groupNode) throws Exception {
                return CacheBuilder.newBuilder().build(new CacheLoader<TableMeta, ITable>() {

                    @Override
                    public ITable load(TableMeta meta) throws Exception {
                        try {
                            HbTable table = new HbTable(meta, groupNameAndExecutors.get(groupNode)
                                .getRemotingExecutableObject(), physicalSchema.get(meta.getTableName()));
                            return table;
                        } catch (Exception ex) {
                            throw new TddlNestableRuntimeException(ex);
                        }
                    }

                });
            }
        });

        executors = CacheBuilder.newBuilder().build(new CacheLoader<Group, IGroupExecutor>() {

            @Override
            public IGroupExecutor load(Group group) throws Exception {

                HBaseGroupExecutor executor = new HBaseGroupExecutor(getRepo());
                group.getProperties().put(HbaseConf.cluster_name, group.getName());
                executor.setGroup(group);
                executor.setHbOperate(getHBCluster(group.getProperties()));

                groupNameAndExecutors.put(group.getName(), executor);

                return executor;
            }
        });
    }

    protected IRepository getRepo() {
        return this;
    }
}
