package com.taobao.tddl.optimizer.exception;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * @author jianghang 2013-11-12 下午2:25:55
 * @since 5.0.0
 */
public class SqlParserException extends TddlRuntimeException {

    private static final long serialVersionUID = 6432150590171245275L;

    public SqlParserException(String... params){
        super(ErrorCode.ERR_PARSER, params);
    }

    public SqlParserException(Throwable cause, String... params){
        super(ErrorCode.ERR_PARSER, cause, params);
    }

    public SqlParserException(Throwable cause){
        super(ErrorCode.ERR_PARSER, cause, new String[] { cause.getMessage() });
    }

}
