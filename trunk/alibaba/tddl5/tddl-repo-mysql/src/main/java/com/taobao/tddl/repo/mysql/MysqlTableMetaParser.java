package com.taobao.tddl.repo.mysql;

import java.util.List;

import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;

public class MysqlTableMetaParser extends TableMetaParser {

    @Override
    protected TableMeta newTableMeta(String tableName, List<ColumnMeta> columns, IndexMeta primaryIndex,
                                     List<IndexMeta> indexs) {
        return new MysqlTableMeta(tableName, columns, primaryIndex, indexs);
    }

}
