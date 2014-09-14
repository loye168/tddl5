package com.taobao.tddl.optimizer.exception;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class OptimizerException extends TddlRuntimeException {

    private static final long serialVersionUID = 4520487604630799374L;

    public OptimizerException(String... params){
        super(ErrorCode.ERR_OPTIMIZER, params);
    }

    public OptimizerException(Throwable cause, String... params){
        super(ErrorCode.ERR_OPTIMIZER, cause, params);
    }

    public OptimizerException(Throwable cause){
        super(ErrorCode.ERR_OPTIMIZER, cause, new String[] { cause.getMessage() });
    }

}
