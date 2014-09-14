package com.taobao.tddl.repo.oceanbase.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.transaction.StrictlTransaction;

/**
 * @author dreamond 2014年1月9日 下午4:58:58
 * @since 5.0.0
 */
public class Ob_Transaction extends StrictlTransaction {

    public Ob_Transaction(ExecutionContext executionContext) throws TddlException{
        super(executionContext);
    }

    protected final static Logger logger = LoggerFactory.getLogger(Ob_Transaction.class);

}
