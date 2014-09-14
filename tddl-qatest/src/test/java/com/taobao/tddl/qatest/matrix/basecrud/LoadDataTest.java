package com.taobao.tddl.qatest.matrix.basecrud;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

public class LoadDataTest extends BaseMatrixTestCase {

    private static String path = Thread.currentThread().getContextClassLoader().getResource(".").getPath();

    @BeforeClass
    public static void prepareLocalFile() {

        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            for (int i = 0; i < 300; i++) {
                String str = String.format("%d|%d|2012-%02d-%02d|2012-%02d-%02d|2012-%02d-%02d|chenhui%d|0.%d\n",
                    i,
                    i,
                    i / 30 + 1,
                    i % 29 + 1,
                    i / 30 + 1,
                    i % 29 + 1,
                    i / 30 + 1,
                    i % 29 + 1,
                    i,
                    i);

                fw.write(str);
            }

            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void deleteLocalFile() {
        File f = new File(path + "localdata.txt");
        if (f.exists()) {
            f.delete();
        }
    }

    // @Parameters(name = "{index}:table={0}")
    // public static List<String[]> prepareData() {
    // return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    // }

    public LoadDataTest(){
        BaseTestCase.normaltblTableName = "MYSQL_NORMALTBL_ONEGROUP_ONEATOM";
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
    }

    @Test
    public void testLoadData() throws Exception {
        String sql = String.format("load data local infile '%slocaldata.txt' into table %s fields terminated by '|' LINES terminated by '\\n';",
            path,
            normaltblTableName);
        System.out.println(path);
        execute(sql, null);
        sql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP",
                "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    // @Ignore
    @Test
    public void testLoadDataNotExistFile() {
        String sql = String.format("load data local infile '%sabc.txt' into table %s fields terminated by '|' LINES terminated by '\\n';",
            path,
            normaltblTableName);
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to open file"));
        }

    }

    @Test
    public void testLoadDataNotExistTable() {
        String sql = String.format("load data local infile '%slocaldata.txt' into table %s fields terminated by '|' LINES terminated by '\\n';",
            path,
            "not_exist_table");
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not found table : NOT_EXIST_TABLE"));
        }

    }

    @Test
    public void testLoadDataMultiGroup() {
        String sql = String.format("load data local infile '%slocaldata.txt' into table %s fields terminated by '|' LINES terminated by '\\n';",
            path,
            "MYSQL_NORMALTBL_MUTILGROUP");
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage()
                .contains("optimize error by load data support single group and single table only"));
        }

    }

    @Test
    public void testLoadDataMultiAtom() {
        String sql = String.format("load data local infile '%slocaldata.txt' into table %s fields terminated by '|' LINES terminated by '\\n';",
            path,
            "MYSQL_NORMALTBL_ONEGROUP_MUTILATOM");
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage()
                .contains("optimize error by load data support single group and single table only"));
        }

    }

}
