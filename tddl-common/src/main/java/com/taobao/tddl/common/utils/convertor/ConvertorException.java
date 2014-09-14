package com.taobao.tddl.common.utils.convertor;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * 类型转化异常
 * 
 * @author jianghang 2014-4-23 下午1:48:19
 * @since 5.1.0
 */
public class ConvertorException extends TddlRuntimeException {

    private static final long serialVersionUID = 3270080439323296921L;

    public ConvertorException(String... params){
        super(ErrorCode.ERR_CONVERTOR, params);
    }

    public ConvertorException(Throwable cause, String... params){
        super(ErrorCode.ERR_CONVERTOR, cause, params);
    }

    public ConvertorException(Throwable cause){
        super(ErrorCode.ERR_CONVERTOR, cause, new String[] { cause.getMessage() });
    }

}
