package com.taobao.tddl.client.sequence.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.client.sequence.exception.SequenceException;

public class RandomSequenceTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test_randomIntSequence() {
        try {
            RandomSequence.randomIntSequence(0);
            Assert.assertTrue(false);
        } catch (SequenceException e) {
            Assert.assertTrue(true);
        }
        try {
            RandomSequence.randomIntSequence(-1);
            Assert.assertTrue(false);
        } catch (SequenceException e) {
            Assert.assertTrue(true);
        }
        int[] random1;
        try {
            random1 = RandomSequence.randomIntSequence(1);
            Assert.assertEquals(random1[0], 0);
        } catch (SequenceException e) {
        }

    }
}
