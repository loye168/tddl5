package com.alibaba.cobar.statistics;

import com.taobao.tddl.statistics.SQLRecord;
import com.taobao.tddl.statistics.SQLRecorder;

/**
 * @author xianmao.hexm
 */
public class SQLStatisticsMain {

    public void performanc() {
        SQLRecorder sqlStatistics = new SQLRecorder(10, true);
        for (int i = 0; i < 1000000; i++) {
            if (sqlStatistics.check(i)) {
                SQLRecord recorder = new SQLRecord();
                recorder.executeTime = i;
                sqlStatistics.add(recorder);
            }
        }
    }

    public static void main(String[] args) {
        SQLStatisticsMain test = new SQLStatisticsMain();
        test.performanc();
    }

}
