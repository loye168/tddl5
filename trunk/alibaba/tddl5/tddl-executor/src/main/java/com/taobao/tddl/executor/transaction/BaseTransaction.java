package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.executor.common.AtomicNumberCreator;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.spi.ITransaction;

public abstract class BaseTransaction implements ITransaction {

    protected final AtomicNumberCreator idGen = AtomicNumberCreator.getNewInstance();
    protected final Integer             id    = idGen.getIntegerNextNumber();
    protected ExecutionContext          executionContext;
    private boolean                     closed;

    protected final ReentrantLock       lock  = new ReentrantLock();

    public BaseTransaction(ExecutionContext executionContext){
        super();
        this.executionContext = executionContext;
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;

    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void close() {

        this.closed = true;

    }

    @Override
    public boolean isClosed() {
        lock.lock();

        try {
            return this.closed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void kill() throws SQLException {
        lock.lock();

        try {
            this.getConnectionHolder().kill();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void cancel() throws SQLException {
        lock.lock();

        try {
            this.getConnectionHolder().cancel();
        } finally {
            lock.unlock();
        }
    }

}
