/**
 * (created at 2011-10-19)
 */
package com.alibaba.cobar.util;

import java.util.Set;


import org.junit.Assert;
import org.junit.Test;


/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class PermutationUtilTest {

    @Test
    public void testPermutate() {
        Set<String> set = PermutationUtil.permutateSQL("-", "1");
        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains("1"));

        set = PermutationUtil.permutateSQL("-", "1", "1");
        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains("1-1"));

        set = PermutationUtil.permutateSQL("-", "1", "2");
        Assert.assertEquals(2, set.size());
        Assert.assertTrue(set.contains("1-2"));
        Assert.assertTrue(set.contains("2-1"));

        set = PermutationUtil.permutateSQL("-", "1", "2", "2");
        Assert.assertEquals(3, set.size());
        Assert.assertTrue(set.contains("1-2-2"));
        Assert.assertTrue(set.contains("2-1-2"));
        Assert.assertTrue(set.contains("2-2-1"));

        set = PermutationUtil.permutateSQL("-", "1", "2", "3");
        Assert.assertEquals(6, set.size());
        Assert.assertTrue(set.contains("1-2-3"));
        Assert.assertTrue(set.contains("1-3-2"));
        Assert.assertTrue(set.contains("2-1-3"));
        Assert.assertTrue(set.contains("2-3-1"));
        Assert.assertTrue(set.contains("3-2-1"));
        Assert.assertTrue(set.contains("3-1-2"));
    }

    @Test
    public void testPermutateNull() {
        try {
            PermutationUtil.permutateSQL("-");
            Assert.assertFalse(true);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
        } catch (Throwable t) {
            Assert.assertFalse(true);
        }
    }

}
