package com.taobao.tddl.repo.hbase.cursor;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.rowset.AbstractRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-9-20下午02:05:20
 */
public class MapRowSet extends AbstractRowSet implements IRowSet {

    private Map<Integer, Object> row;

    public MapRowSet(int capacity, ICursorMeta iCursorMeta){
        super(iCursorMeta);
        row = new HashMap<Integer, Object>(capacity);
    }

    public MapRowSet(ICursorMeta iCursorMeta, Map<Integer, Object> row){
        super(iCursorMeta);
        this.row = row;
    }

    @Override
    public Object getObject(int index) {
        return row.get(index);
    }

    @Override
    public void setObject(int index, Object value) {
        row.put(index, value);
    }
}
