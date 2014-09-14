package com.taobao.tddl.repo.bdb.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.optimizer.config.table.TableMeta;

public class JE_TempTable extends JE_Table implements ITempTable {

    /**
     * 临时表返回给前段的meta
     */
    ICursorMeta returnMeta = null;

    public JE_TempTable(TableMeta schema, IRepository repo){
        super(schema, repo);
    }

    @Override
    public ICursorMeta getCursorMeta() {
        return this.returnMeta;
    }

    @Override
    public void setCursorMeta(ICursorMeta cursorMeta) {
        this.returnMeta = cursorMeta;
    }

    @Override
    public ISchematicCursor getCursor(ExecutionContext context) throws TddlException {
        return super.getCursor(context, this.getSchema().getPrimaryIndex(), this.getSchema().getTableName());
    }

}
