package com.alibaba.cobar.parser.perf;

import com.alibaba.cobar.manager.parser.ManagerParse;

/**
 * @author xianmao.hexm
 */
public class ManagerParserPerformanceMain {

    public void testPerformance() {
        for (int i = 0; i < 250000; i++) {
            ManagerParse.parse("show databases");
            ManagerParse.parse("set autocommit=1");
            ManagerParse.parse(" show  @@datasource ");
            ManagerParse.parse("select id,name,value from t");
        }
    }

    public void testPerformanceWhere() {
        for (int i = 0; i < 500000; i++) {
            ManagerParse.parse(" show  @@datasource where datanode = 1");
            ManagerParse.parse(" show  @@datanode where schema = 1");
        }
    }

}
