package com.alibaba.cobar.server.exception;

/**
 * @author xianmao.hexm
 */
public class HeartbeatException extends RuntimeException {

    private static final long serialVersionUID = 7639414445868741580L;

    public HeartbeatException(){
        super();
    }

    public HeartbeatException(String message, Throwable cause){
        super(message, cause);
    }

    public HeartbeatException(String message){
        super(message);
    }

    public HeartbeatException(Throwable cause){
        super(cause);
    }

}
