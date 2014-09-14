package com.taobao.tddl.rule.enumerator;

import static com.taobao.tddl.rule.TestUtils.GreaterThanOrEqual;
import static com.taobao.tddl.rule.TestUtils.LessThan;
import static com.taobao.tddl.rule.TestUtils.LessThanOrEqual;
import static com.taobao.tddl.rule.TestUtils.gand;
import static com.taobao.tddl.rule.TestUtils.gcomp;
import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import org.junit.Test;

import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.rule.TestUtils;
import com.taobao.tddl.rule.model.DateEnumerationParameter;

public class DatePartDiscontinousRangeEnumeratorYearUnitTest {

    /*
     * T:测试在有自增和没有自增的情况下 对于close interval的处理， 在有自增和range的情况下测试 x = ? or (x > ?
     * and x < ?) 测试开区间 ，测试x>5 and x>10,测试x >= 3 and x < 5取值是否正确 测试x>3 and
     * x<5取值是否正确。 测试x >=3 and x=3的时候返回是否正确。
     */
    Comparative btc                        = null;
    Enumerator  e                          = new EnumeratorImp();
    // @Before
    // public void setUp() throws Exception{
    // e.setNeedMergeValueInCloseInterval(true);
    // }
    boolean     needMergeValueInCloseRange = true;

    @Test
    public void test_带有自增的TC_在时间范围内() throws Exception {
        btc = gand(gcomp(getDate(109, 00, 01), GreaterThanOrEqual),
            gcomp(getDate(109, 11, 31, 23, 59, 59), LessThanOrEqual));
        DateEnumerationParameter pa = new DateEnumerationParameter(5/* 5年 */, Calendar.YEAR);
        Set<Object> s = e.getEnumeratedValue(btc, 1, pa, needMergeValueInCloseRange);
        // 还在一个日期里，实际上是毫秒数+1了，变为表的时候是不会显示毫秒数的
        TestUtils.testSetDate(new Date[] { getDate(109, 00, 01), getDate(109, 11, 31, 23, 59, 59) }, s);
        assertDate(s, 2009);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test_带有自增的Tc_在时间范围内() throws Exception {
        btc = gand(gcomp(getDate(109, 00, 01), GreaterThanOrEqual), gcomp(new Date(110, 00, 01), LessThan));
        DateEnumerationParameter pa = new DateEnumerationParameter(5/* 5年 */, Calendar.YEAR);
        Set<Object> s = e.getEnumeratedValue(btc, 1, pa, needMergeValueInCloseRange);
        // 还在一个日期里，实际上是毫秒数+1了，变为表的时候是不会显示毫秒数的
        TestUtils.testSetDate(new Date[] { getDate(109, 00, 01), getDate(109, 11, 31, 23, 59, 59) }, s);
        assertDate(s, 2009);
    }

    private void assertDate(Set<Object> s, int year) {
        Calendar cal = Calendar.getInstance();
        for (Object d : s) {
            Date da = (Date) d;
            cal.setTime(da);
            assertEquals(cal.get(Calendar.YEAR), year);
        }
    }

    /*
     * @Test public void test_超出时间范围内()throws Exception{ btc = gcomp(new
     * Date(109,00,01), GreaterThanOrEqual); DateEnumerationParameter pa = new
     * DateEnumerationParameter(1,Calendar.YEAR); Set<Object> s =
     * e.getEnumeratedValue(btc,5,pa,needMergeValueInCloseRange);
     * //还在一个日期里，实际上是毫秒数+1了，变为表的时候是不会显示毫秒数的 TestUtils.testSetDate(new Date[]{new
     * Date(109,00,01),new Date(109,11,31,23,59,59)},s ); }
     */

    // --------------------------------------------------以下是一些对两个and节点上挂两个参数一些情况的单元测试。
    // 因为从公共逻辑测试中已经测试了> 在处理中会转变为>= 而< 在处理中会转为<= 因此只需要测试> = <
    // 在and,两个节点的情况下的可能性即可。

    @SuppressWarnings("deprecation")
    private Date getDate(int year, int month, int date) {
        return new Date(year, month, date);
    }

    @SuppressWarnings("deprecation")
    public Date getDate(int year, int month, int date, int hrs, int min, int sec) {
        return new Date(year, month, date, hrs, min, sec);
    }
}
