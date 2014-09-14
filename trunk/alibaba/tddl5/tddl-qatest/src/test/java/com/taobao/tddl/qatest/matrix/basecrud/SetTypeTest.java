package com.taobao.tddl.qatest.matrix.basecrud;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;

public class SetTypeTest extends BaseMatrixTestCase {

    public SetTypeTest(){
        normaltblTableName = "mysql_set_type";
    }

    @Before
    public void prepare() throws Exception {
        tddlUpdateData("delete from " + normaltblTableName, null);
        mysqlUpdateData("delete from " + normaltblTableName, null);
    }

    @Test
    public void testInsertOneValue() throws Exception {
        String sql = "insert into " + normaltblTableName + "(setCol,pk) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID);
        execute(sql, param);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "setCol" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testInsertMultiValue() throws Exception {
        String sql = "insert into " + normaltblTableName + "(setCol,pk) values(?,?),(?,?),(?,?),(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 1);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 2);

        param.add("a,b,chenhui,hello");
        param.add(RANDOM_ID + 3);
        execute(sql, param);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "setCol" };
        // selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    @Ignore("kelude跑不过")
    public void testInsertWithWrongValue() {
        String sql = "insert into " + normaltblTableName + "(setCol,pk) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("a,b,chenhui,world");
        param.add(RANDOM_ID);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Data truncated for column 'setCol' at row 1");
        }
    }

    @Test
    public void testInsertMultiWithMissingValue() throws Exception {
        String sql = "insert into " + normaltblTableName + "(setCol,pk) values(?,?),(?,?),(?,?),(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("b,chenhui,hello");
        param.add(RANDOM_ID);

        param.add("a,chenhui,hello");
        param.add(RANDOM_ID + 1);

        param.add("a,b,hello");
        param.add(RANDOM_ID + 2);

        param.add("a,b,chenhui");
        param.add(RANDOM_ID + 3);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "setCol" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testInsertMultiValueWithNormalSql() throws Exception {
        String sql = "insert into " + normaltblTableName + "(setCol,pk) values(?,?),(?,?),(?,?),(?,?)";
        sql = String.format("insert into %s(setCol,pk) values(%s,%d),(%s,%d),(%s,%d),(%s,%d)",
            normaltblTableName,
            "'a,b,chenhui,hello'",
            RANDOM_ID,
            "'a,b,hello'",
            RANDOM_ID + 1,
            "'a,chenhui,hello'",
            RANDOM_ID + 2,
            "'a,b,chenhui'",
            RANDOM_ID + 3);
        execute(sql, null);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "setCol" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }
}
