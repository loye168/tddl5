package com.taobao.tddl.repo.mysql.executor;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.AbstractGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

/**
 * 为TGroupDatasource实现的groupexecutor
 * 因为TGroupDatasource中已经做了主备切换等功能，所以TddlGroupExecutor只是简单的执行sql
 * 
 * @author mengshi.sunmengshi 2013-12-6 下午2:39:18
 * @since 5.0.0
 */
public class TddlGroupExecutor extends AbstractGroupExecutor {

    private TGroupDataSource groupDataSource;

    public TddlGroupExecutor(IRepository repo){
        super(repo);
    }

    @Override
    protected void doInit() throws TddlException {
        super.doInit();
    }

    @Override
    protected void doDestroy() throws TddlException {
        if (this.groupDataSource != null) {
            try {
                groupDataSource.destroyDataSource();
            } catch (Exception e) {
                throw new TddlException(e);
            }
        }
    }

    @Override
    public TGroupDataSource getRemotingExecutableObject() {
        return groupDataSource;
    }

    public void setGroupDataSource(TGroupDataSource ds) {
        this.groupDataSource = ds;
    }

}
