package com.taobao.tddl.qatest.atom;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;

import com.taobao.tddl.qatest.util.DateUtil;

/**
 * Comment for AtomBaseCRUDTest
 * <p/>
 */
@SuppressWarnings("rawtypes")
public class AtomBaseCRUDTest extends AtomTestCase {

    /**
     * 在指定的表中插入数据
     */
    @Test
    public void insertByPreStTest() {
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        Object[] arguments = new Object[] { RANDOM_ID, time };
        tddlJT.update(sql, arguments);

        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    /**
     * 在指定的表更新数据
     */
    @Test
    public void updateByPreStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        Object[] arguments = new Object[] { nextDay, RANDOM_ID };
        tddlJT.update(sql, arguments);

        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }

    /**
     * 在指定的表删除数据
     */
    @Test
    public void queryByPreStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        String sql = "select * from normaltbl_0001 where pk=?";
        Object[] arguments = new Object[] { RANDOM_ID };
        Map re = tddlJT.queryForMap(sql, arguments);
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    /**
     * 在指定的表查询数据
     */
    @Test
    public void deleteByPreStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        String sql = "delete from normaltbl_0001 where pk=?";
        Object[] arguments = new Object[] { RANDOM_ID };
        tddlJT.update(sql, arguments);

        List re = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, re.size());
    }

    /**
     * 在指定的表进行replace操作
     */
    @Test
    public void replaceByPreStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });
        String sql = "replace into normaltbl_0001 (pk,gmt_create) values (?,?)";
        Object[] arguments = new Object[] { RANDOM_ID, nextDay };
        tddlJT.update(sql, arguments);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }

    /**
     * 在指定的表中插入数据
     */
    @Test
    public void insertByStTest() {
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",CURDATE())";
        tddlJT.update(sql);

        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=" + RANDOM_ID);
        Assert.assertEquals(time, DateUtil.formatDate((Date) re.get("gmt_create"), DateUtil.DATE_FULLHYPHEN));
    }

    /**
     * 在指定的表更新数据
     */
    @Test
    public void updateByStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        String sql = "update normaltbl_0001 set gmt_create= ADDDATE(CURDATE(),INTERVAL 1 DAY) where pk=" + RANDOM_ID;
        tddlJT.update(sql);

        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=" + RANDOM_ID);
        Assert.assertEquals(nextDay, DateUtil.formatDate((Date) re.get("gmt_create"), DateUtil.DATE_FULLHYPHEN));
    }

    /**
     * 在指定的表删除数据
     */
    @Test
    public void queryByStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=" + RANDOM_ID);
        Assert.assertEquals(time, DateUtil.formatDate((Date) re.get("gmt_create"), DateUtil.DATE_FULLHYPHEN));
    }

    /**
     * 在指定的表查询数据
     */
    @Test
    public void deleteByStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });

        String sql = "delete from normaltbl_0001 where pk=" + RANDOM_ID;
        tddlJT.update(sql);

        List re = tddlJT.queryForList("select * from normaltbl_0001 where pk=" + RANDOM_ID);
        Assert.assertEquals(0, re.size());
    }

    /**
     * 在指定的表进行replace操作
     */
    @Test
    public void replaceByStTest() {
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID, time });
        String sql = "replace into normaltbl_0001 (gmt_create,pk) values (ADDDATE(CURDATE(),INTERVAL 1 DAY),"
                     + RANDOM_ID + ")";
        tddlJT.update(sql);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=" + RANDOM_ID);
        Assert.assertEquals(nextDay, DateUtil.formatDate((Date) re.get("gmt_create"), DateUtil.DATE_FULLHYPHEN));
    }

    /**
     * 不存在的表数据操作
     */
    @Test
    public void noneExistTblTest() {
        String time = DateUtil.formatDate(new Date(), DateUtil.DATE_FULLHYPHEN);
        String sql = "insert into normaltbl_noneExist (pk,gmt_create) values (?,?)";
        try {
            tddlJT.update(sql, new Object[] { RANDOM_ID, time });
            Assert.fail();
        } catch (DataAccessException e) {
        }
    }
}
