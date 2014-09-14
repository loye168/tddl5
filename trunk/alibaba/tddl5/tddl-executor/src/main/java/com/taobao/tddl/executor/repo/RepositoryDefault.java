package com.taobao.tddl.executor.repo;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.spi.CursorFactoryDefaultImpl;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.optimizer.config.table.TableMeta;

public class RepositoryDefault extends AbstractLifecycle implements IRepository {

    private ICursorFactory cursorFactory = new CursorFactoryDefaultImpl();

    @Override
    public ITempTable getTempTable(TableMeta meta) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ITable getTable(TableMeta meta, String groupNode, String actualTableName) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RepositoryConfig getRepoConfig() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWriteAble() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isEnhanceExecutionModel(String groupKey) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        // TODO Auto-generated method stub
        return this.cursorFactory;
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IGroupExecutor getGroupExecutor(Group group) {
        // TODO Auto-generated method stub
        return null;
    }

}
