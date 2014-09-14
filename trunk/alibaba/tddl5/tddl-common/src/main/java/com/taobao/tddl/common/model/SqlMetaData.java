package com.taobao.tddl.common.model;

import java.util.ArrayList;
import java.util.List;

public class SqlMetaData {

    // 针对格式或者参数个数不同，但表达语义一致的sql的统一格式化 例如 id in(?,?...) 统一为 id in (?)
    private String       logicSql;
    private String       index;
    private List<String> logicTables = new ArrayList<String>();

    public String getLogicSql() {
        return logicSql;
    }

    public void setLogicSql(String logicSql) {
        this.logicSql = logicSql;
    }

    public List<String> getLogicTables() {
        return logicTables;
    }

    public void setLogicTables(List<String> logicTables) {
        this.logicTables = logicTables;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

}
