package com.taobao.tddl.statistics;

import java.util.Map;

/**
 * @author xianmao.hexm
 */
public final class SQLRecord implements Comparable<SQLRecord> {

    public String              host;
    public String              schema;
    public String              statement;
    public Map<String, String> tableNames;
    public long                startTime;
    public long                executeTime;
    public String              dataNode;
    public int                 dataNodeIndex;

    @Override
    public int compareTo(SQLRecord o) {
        return (int) (executeTime - o.executeTime);
    }

}
