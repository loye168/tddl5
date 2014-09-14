package com.taobao.tddl.repo.mysql;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;

public class MysqlTableMeta extends TableMeta {

    private static final long serialVersionUID = 1530693811588739202L;

    public MysqlTableMeta(String tableName, List<ColumnMeta> allColumnsOrderByDefined, IndexMeta primaryIndex,
                          List<IndexMeta> secondaryIndexes){
        super(tableName, allColumnsOrderByDefined, primaryIndex, secondaryIndexes);
    }

    @Override
    public List<IndexMeta> getIndexs() {

        return new ArrayList();
    }
}
