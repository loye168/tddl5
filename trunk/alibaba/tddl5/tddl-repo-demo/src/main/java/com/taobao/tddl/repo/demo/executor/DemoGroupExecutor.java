package com.taobao.tddl.repo.demo.executor;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.AbstractGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;

/**
 * @author mengshi.sunmengshi 2014年4月10日 下午5:17:13
 * @since 5.1.0
 */
public class DemoGroupExecutor extends AbstractGroupExecutor {

    public DemoGroupExecutor(IRepository repo){
        super(repo);
    }

    @Override
    protected void doInit() throws TddlException {
        super.doInit();
    }

    @Override
    protected void doDestroy() throws TddlException {

    }

    @Override
    public Object getRemotingExecutableObject() {
        return null;
    }

}
