package com.taobao.tddl.repo.hbase.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.repo.hbase.model.HbData;
import com.taobao.tddl.repo.hbase.model.HbData.HbColumn;

/**
 * HBase get/put/delete/scan的操作接口
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @author jianghang 2013-7-26 下午5:08:47
 * @since 3.0.1
 */
public class HbOperate extends AbstractLifecycle {

    private HbFactory        factory;
    private volatile boolean locked    = false;
    private volatile boolean available = true;

    @Override
    public void doInit() throws TddlException {
        if (factory == null) {
            throw new IllegalArgumentException("HbFactory is null");
        }
        factory.init();
    }

    @Override
    public void doDestroy() throws TddlException {
        if (factory != null) {
            factory.destroy();
        }
    }

    public HbOperate(HbFactory factory){
        this.factory = factory;
    }

    void setNotAvailable() {
        this.available = false;
    }

    boolean isLocked() {
        return locked;
    }

    boolean isAvailable() {
        return this.available;
    }

    void checkAvailableAndGetLock() {

        if (this.isAvailable()) {
            return;
        }

        // 只让一个线程去尝试，拿不到锁的线程直接返回错误
        if (locked) {
            throw new RuntimeException("hbase is not available, please check and try again later");
        }

        synchronized (this) {
            // double check
            if (locked) {
                throw new RuntimeException("hbase is not available, please check and try again later");
            }
            locked = true;
        }
    }

    void clearLock() {
        this.locked = false;
        this.available = true;
    }

    public void mput(List<HbData> opDatas) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opDatas);
            Cache<byte[], Put> puts = CacheBuilder.newBuilder().build();
            for (HbData opData : opDatas) {
                final byte[] rowKey = opData.getRowKey();
                Put put = puts.get(rowKey, new Callable<Put>() {

                    @Override
                    public Put call() throws Exception {
                        return new Put(rowKey);
                    }
                });

                for (HbColumn column : opData.getColumns()) {
                    byte[] columnFamily = Bytes.toBytes(column.getColumnFamily());
                    byte[] columnName = Bytes.toBytes(column.getColumnName());
                    byte[] value = column.getValue();
                    if (column.getTimestamp() > 0) {
                        put.add(new KeyValue(rowKey, columnFamily, columnName, column.getTimestamp(), value));
                    } else {
                        put.add(new KeyValue(rowKey, columnFamily, columnName, value));
                    }
                }
            }

            htable.put(new ArrayList<Put>(puts.asMap().values()));
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("put data to hbase fail with table(" + opDatas.get(0).getTableName() + ")", e);
        } catch (Throwable e) {
            throw new RuntimeException("put data to hbase fail with table(" + opDatas.get(0).getTableName() + ")", e);
        } finally {
            closeHtable(htable, opDatas.get(0));
        }

        clearLock();
    }

    public void put(HbData opData) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opData);
            byte[] rowKey = opData.getRowKey();
            Put put = new Put(rowKey);
            for (HbColumn column : opData.getColumns()) {
                byte[] columnFamily = Bytes.toBytes(column.getColumnFamily());
                byte[] columnName = Bytes.toBytes(column.getColumnName());
                byte[] value = column.getValue();
                if (column.getTimestamp() > 0) {
                    put.add(new KeyValue(rowKey, columnFamily, columnName, column.getTimestamp(), value));
                } else {
                    put.add(new KeyValue(rowKey, columnFamily, columnName, value));
                }
            }
            htable.put(put);
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("put data to hbase fail with table(" + opData.getTableName() + ")", e);
        } catch (Throwable e) {
            throw new RuntimeException("put data to hbase fail with table(" + opData.getTableName() + ")", e);
        } finally {
            closeHtable(htable, opData);
        }

        clearLock();
    }

    public Result[] mget(List<HbData> opDatas) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opDatas);
            List<Get> getList = new LinkedList<Get>();
            for (HbData hbData : opDatas) {
                getList.add(buildGet(hbData));
            }

            Result[] results = htable.get(getList);

            clearLock();
            return results;
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("get data from hbase fail with table(" + opDatas.get(0).getTableName() + ")", e);
        } catch (Throwable e) {
            throw new RuntimeException("get data from hbase fail with table(" + opDatas.get(0).getTableName() + ")", e);
        } finally {
            closeHtable(htable, opDatas.get(0));
        }

    }

    public Result get(HbData opData) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opData);
            Get get = buildGet(opData);
            Result result = htable.get(get);

            clearLock();
            return result;
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("get data from hbase fail with table(" + opData.getTableName() + ")", e);
        } catch (Throwable e) {
            throw new RuntimeException("get data from hbase fail with table(" + opData.getTableName() + ")", e);
        } finally {
            closeHtable(htable, opData);
        }

    }

    public void mdelete(List<HbData> opDatas) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opDatas);
            Cache<byte[], Delete> deletes = CacheBuilder.newBuilder().build();
            for (HbData opData : opDatas) {
                final byte[] rowKey = opData.getRowKey();
                Delete delete = deletes.get(rowKey, new Callable<Delete>() {

                    @Override
                    public Delete call() throws Exception {
                        return new Delete(rowKey);
                    }
                });

                for (HbColumn column : opData.getColumns()) {
                    if (column.getTimestamp() > 0) {
                        delete.deleteColumn(Bytes.toBytes(column.getColumnFamily()),
                            Bytes.toBytes(column.getColumnName()),
                            column.getTimestamp());
                    } else {
                        delete.deleteColumns(Bytes.toBytes(column.getColumnFamily()),
                            Bytes.toBytes(column.getColumnName()));
                    }
                }
            }

            htable.delete(new ArrayList<Delete>(deletes.asMap().values()));
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("delete data from hbase fail with table(" + opDatas.get(0).getTableName() + ")",
                e);
        } catch (Throwable e) {
            throw new RuntimeException("delete data from hbase fail with table(" + opDatas.get(0).getTableName() + ")",
                e);
        } finally {
            closeHtable(htable, opDatas.get(0));
        }

        clearLock();
    }

    public void delete(HbData opData) {
        checkAvailableAndGetLock();
        HTableInterface htable = null;
        try {
            htable = getHtable(opData);
            Delete delete = new Delete(opData.getRowKey());

            for (HbColumn column : opData.getColumns()) {
                if (column.getTimestamp() > 0) {
                    delete.deleteColumn(Bytes.toBytes(column.getColumnFamily()),
                        Bytes.toBytes(column.getColumnName()),
                        column.getTimestamp());
                } else {
                    delete.deleteColumns(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
                }
            }

            htable.delete(delete);
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("delete data from hbase fail with table(" + opData.getTableName() + ")", e);
        } catch (Throwable e) {
            throw new RuntimeException("delete data from hbase fail with table(" + opData.getTableName() + ")", e);
        } finally {
            closeHtable(htable, opData);
        }

        clearLock();
    }

    static AtomicInteger k = new AtomicInteger(0);

    /**
     * 构造Scan扫描器
     * 
     * @param opData
     * @return
     */
    public ResultScanner scan(HbData opData) {
        checkAvailableAndGetLock();
        HTableInterface scanTable = null;
        try {
            scanTable = getHtable(opData);

            Scan s = new Scan();
            // s.setBatch(scanBatchSize);

            // 设置startRow/endRwo
            if (opData.getStartRow() != null) {
                s.setStartRow(opData.getStartRow());
            }
            if (opData.getEndRow() != null) {
                s.setStopRow(opData.getEndRow());
            }

            // 设置versions
            if (opData.getMaxVersion() > 0) {
                s.setMaxVersions(opData.getMaxVersion());
            }

            // 设置startTime/endTime
            if (opData.getStartTime() > 0 && opData.getEndTime() > 0 && opData.getEndTime() > opData.getStartTime()) {
                s.setTimeRange(opData.getStartTime(), opData.getEndTime());
            }

            for (HbColumn column : opData.getColumns()) {
                if (StringUtils.isNotEmpty(column.getColumnFamily()) && StringUtils.isNotEmpty(column.getColumnName())) {
                    s.addColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
                }
            }

            ResultScanner sc = scanTable.getScanner(s);

            // int value = k.getAndIncrement();
            // if ( k.get()>=10 && k.get()<=30) {
            // Thread.sleep(10000);
            // System.out.println();
            // throw new IOException("test exception");
            // }

            clearLock();

            return sc;
        } catch (IOException e) {
            setNotAvailable();
            throw new RuntimeException("get data from hbase fail with table(" + opData.getTableName() + ")", e);
        } catch (Throwable e) {

            throw new RuntimeException("get data from hbase fail with table(" + opData.getTableName() + ")", e);
        } finally {
            closeHtable(scanTable, opData);
        }

    }

    // ================

    private Get buildGet(HbData opData) throws IOException {
        Get get = new Get(opData.getRowKey());

        if (opData.getMaxVersion() > 0) {
            get.setMaxVersions(opData.getMaxVersion());
        }

        for (HbColumn column : opData.getColumns()) {
            if (column.getTimestamp() > 0) {
                get.setTimeStamp(column.getTimestamp());
            } else if (opData.getStartTime() > 0 && opData.getEndTime() > 0
                       && opData.getEndTime() > opData.getStartTime()) {
                get.setTimeRange(opData.getStartTime(), opData.getEndTime());
            }

            if (StringUtils.isNotEmpty(column.getColumnFamily()) && StringUtils.isNotEmpty(column.getColumnName())) {
                get.addColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
            }
        }
        return get;
    }

    private HTableInterface getHtable(List<HbData> opDatas) {
        HTableInterface htable = opDatas.size() > 0 ? getHtable(opDatas.get(0)) : null;
        if (null == htable) {
            throw new RuntimeException("htable not found.");
        }
        return htable;
    }

    private HTableInterface getHtable(HbData opData) {
        if (null == opData.getTableName()) {
            throw new RuntimeException("tableName can't be null");
        }

        HTableInterface htable = factory.getHtable(opData.getTableName());
        if (null == htable) {
            throw new RuntimeException("htable('" + opData.getTableName() + "' not found.");
        }

        return htable;
    }

    private void closeHtable(HTableInterface htable, HbData opData) {

        this.locked = false;

        if (htable != null) {
            try {
                htable.close();
            } catch (IOException e) {
                throw new RuntimeException("close hbase fail with table(" + opData.getTableName() + ")", e);
            }
        }
    }

    public HbFactory getFactory() {
        return factory;
    }

    public void setFactory(HbFactory factory) {
        this.factory = factory;
    }

}
