package com.taobao.tddl.qatest.matrix.join;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.ExecuteTableName;

/**
 * full join目前不支持
 * 
 * @author zhuoxue
 * @since 5.0.1
 */

@Ignore("目前不支持")
@RunWith(EclipseParameterized.class)
public class FullJoinTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public FullJoinTest(String normaltblTableName, String studentTableName) throws Exception{
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
        prepareDate();
    }

    public void prepareDate() throws Exception {
        tddlUpdateData("delete from " + normaltblTableName, null);
        tddlUpdateData("delete from " + studentTableName, null);
        normaltblPrepare(0, 20);
        studentPrepare(0, 20);
    }

    @After
    public void clearDate() throws Exception {
        if (rc != null) {
            rc.close();
            rc = null;
        }
    }

    @Ignore("目前不支持")
    @Test
    public void fullJoinTest() throws Exception {

    }

}
