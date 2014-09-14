package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.ExecuteTableName;

/**
 * or去重的测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectRemoveDuplicatesTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectRemoveDuplicatesTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void prepareData() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
    }

    /**
     * 条件中带大于or小于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void conditionWithLessAndGreatTest() throws Exception {
        long start = 5;
        int end = 300;

        String sql = "select * from " + normaltblTableName + " where pk >? or id> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        String[] columnParam = { "PK", "NAME", "ID", "GMT_CREATE", "GMT_DATETIME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, param);
    }

}
