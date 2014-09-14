package com.taobao.tddl.repo.demo.spi;

import java.util.NavigableMap;
import java.util.TreeMap;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.demo.cursor.DemoCursor;
import com.taobao.tddl.repo.demo.cursor.DemoUtil;

/**
 * @author mengshi.sunmengshi 2014年4月10日 下午5:16:37
 * @since 5.1.0
 */
public class DemoTable implements ITable {

    private final static String                         DUAL_TABLE = "dual";
    private final NavigableMap<Object, CloneableRecord> map        = new TreeMap<Object, CloneableRecord>();
    private final TableMeta                             tableMeta;

    public DemoTable(TableMeta TableMeta){
        super();
        this.tableMeta = TableMeta;

        if (tableMeta.getTableName().equalsIgnoreCase(DUAL_TABLE)) {
            map.put(1,
                CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                    .getCodec(tableMeta.getPrimaryIndex().getValueColumns())
                    .newEmptyRecord());
        }

    }

    @Override
    public TableMeta getSchema() {
        return tableMeta;
    }

    @Override
    public void close() {
        map.clear();

    }

    @Override
    public void put(ExecutionContext executionContext, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta,
                    String dbName) throws TddlException {

        if (tableMeta.getTableName().equalsIgnoreCase(DUAL_TABLE)) {
            return;
        }
        Object valOfKeys = DemoUtil.getValueOfKey(key);
        map.put(valOfKeys, value);
    }

    @Override
    public void delete(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                                  throws TddlException {

        if (tableMeta.getTableName().equalsIgnoreCase(DUAL_TABLE)) {
            return;
        }
        Object valOfKeys = DemoUtil.getValueOfKey(key);
        map.remove(valOfKeys);

    }

    @Override
    public CloneableRecord get(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta,
                               String dbName) throws TddlException {
        Object valOfKeys = DemoUtil.getValueOfKey(key);
        return map.get(valOfKeys);
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta meta, IQuery iQuery) {
        return this.getCursor(executionContext, meta, meta.getName());
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexMeta, String indexMetaName) {
        DemoCursor dc = new DemoCursor(indexMeta, map);
        return new SchematicCursor(dc, dc.getiCursorMeta(), ExecUtils.getOrderBy(indexMeta));
    }

}
