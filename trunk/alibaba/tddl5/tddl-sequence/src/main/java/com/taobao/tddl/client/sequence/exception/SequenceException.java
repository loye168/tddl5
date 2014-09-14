package com.taobao.tddl.client.sequence.exception;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * @author jianghang 2014-4-23 下午3:44:45
 * @since 5.1.0
 */
public class SequenceException extends TddlException {

    private static final long serialVersionUID = -7383087459057215862L;

    public SequenceException(String... params){
        super(ErrorCode.ERR_SEQUENCE, params);
    }

    public SequenceException(Throwable cause, String... params){
        super(ErrorCode.ERR_SEQUENCE, cause, params);
    }

    public SequenceException(Throwable cause){
        super(cause);
    }

}
