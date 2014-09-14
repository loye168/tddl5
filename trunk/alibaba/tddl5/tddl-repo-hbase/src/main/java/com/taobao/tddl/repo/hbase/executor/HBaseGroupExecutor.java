package com.taobao.tddl.repo.hbase.executor;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.AbstractGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.repo.hbase.operator.HbOperate;

/**
 * 为TGroupDatasource实现的groupexecutor
 * 因为TGroupDatasource中已经做了主备切换等功能，所以TddlGroupExecutor只是简单的执行sql
 * 
 * @author mengshi.sunmengshi 2013-12-6 下午2:39:18
 * @since 5.0.0
 */
public class HBaseGroupExecutor extends AbstractGroupExecutor {

    private HbOperate hbOperate;

    public HBaseGroupExecutor(IRepository repo){
        super(repo);
    }

    @Override
    protected void doInit() throws TddlException {
        super.doInit();
    }

    @Override
    protected void doDestroy() throws TddlException {
        if (this.hbOperate != null) {
            try {
                hbOperate.destroy();
            } catch (Exception e) {
                throw new TddlException(e);
            }
        }
    }

    @Override
    public HbOperate getRemotingExecutableObject() {
        return hbOperate;
    }

    public void setHbOperate(HbOperate hbOperate) {
        this.hbOperate = hbOperate;
    }

}
