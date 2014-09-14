/**
 * (created at 2011-10-31)
 */
package com.alibaba.cobar.util;

import junit.framework.TestCase;

import org.junit.Assert;

import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.server.util.StringUtil;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class StringUtilTest extends TestCase {

    public void testSequenceSlicing() {
        Assert.assertEquals(new Pair<Integer, Integer>(0, 2), StringUtil.sequenceSlicing("2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 2), StringUtil.sequenceSlicing("1: 2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 0), StringUtil.sequenceSlicing(" 1 :"));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), StringUtil.sequenceSlicing("-1: "));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), StringUtil.sequenceSlicing(" -1:0"));
        Assert.assertEquals(new Pair<Integer, Integer>(0, 0), StringUtil.sequenceSlicing(" :"));
    }

}
