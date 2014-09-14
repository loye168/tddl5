package com.alibaba.cobar.server.exception;

/**
 * @author xianmao.hexm
 */
public class ErrorPacketException extends RuntimeException {

    private static final long serialVersionUID = -2692093550257870555L;

    public ErrorPacketException(){
        super();
    }

    public ErrorPacketException(String message, Throwable cause){
        super(message, cause);
    }

    public ErrorPacketException(String message){
        super(message);
    }

    public ErrorPacketException(Throwable cause){
        super(cause);
    }

}
