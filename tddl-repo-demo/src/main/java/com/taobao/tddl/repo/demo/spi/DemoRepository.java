package com.taobao.tddl.repo.demo.spi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
import com.taobao.tddl.repo.demo.executor.DemoGroupExecutor;

/**
 * @author whisper 基于concurrentHashMap的demo实现
 */
public class DemoRepository extends AbstractLifecycle implements IRepository {

    private ICursorFactory                        cursorFactory;
    private ICommandHandlerFactory                commandExecutorFactory;
    private Map<String, DemoTable>                dbMap = new ConcurrentHashMap<String, DemoTable>();
    protected LoadingCache<Group, IGroupExecutor> executors;

    @Override
    public ICursorFactory getCursorFactory() {
        return cursorFactory;
    }

    // @Override
    public void cleanTempTable() {
        // do nothing
    }

    // @Override
    public int cleanLog() {
        // do nothing
        return 0;
    }

    public IRepository getRepo() {
        return this;
    }

    @Override
    public void doInit() throws TddlException {
        this.cursorFactory = new CursorFactoryDemoImp();
        this.commandExecutorFactory = new CommandExecutorFactoryDemoImp();

        executors = CacheBuilder.newBuilder().build(new CacheLoader<Group, IGroupExecutor>() {

            @Override
            public IGroupExecutor load(Group group) throws Exception {

                DemoGroupExecutor executor = new DemoGroupExecutor(getRepo());
                executor.setGroup(group);

                return executor;
            }
        });
    }

    @Override
    public ITempTable getTempTable(TableMeta meta) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ITable getTable(TableMeta meta, String groupNode, String actualTableName) throws TddlException {
        DemoTable table = null;
        table = dbMap.get(actualTableName);
        if (table == null) {
            table = new DemoTable(meta);
            dbMap.put(actualTableName, table);
        }

        return table;
    }

    @Override
    public RepositoryConfig getRepoConfig() {
        // TODO Auto-generated method stub
        return null;
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
    public ICommandHandlerFactory getCommandExecutorFactory() {
        return this.commandExecutorFactory;
    }

    @Override
    public IGroupExecutor getGroupExecutor(Group group) {
        try {
            return executors.get(group);
        } catch (ExecutionException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

}
