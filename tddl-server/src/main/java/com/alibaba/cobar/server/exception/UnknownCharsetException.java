package com.alibaba.cobar.server.exception;

/**
 * 未知字符集异常
 * 
 * @author xianmao.hexm
 */
public class UnknownCharsetException extends RuntimeException {

    private static final long serialVersionUID = 552833416065882969L;

    public UnknownCharsetException(){
        super();
    }

    public UnknownCharsetException(String message, Throwable cause){
        super(message, cause);
    }

    public UnknownCharsetException(String message){
        super(message);
    }

    public UnknownCharsetException(Throwable cause){
        super(cause);
    }

}
