package com.taobao.tddl.client.sequence.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.client.sequence.exception.SequenceException;

public class RandomBalanceTest {

    private static final Long MAX_PERCENTATE = 110L;
    private static final Long MIN_PERCENTATE = 90L;

    @Before
    public void setUp() throws Exception {
    }

    @Ignore
    @Test
    public void test_balance() {
        int ramdomTimes = 100000;
        int randomRange = 100;
        int statisticsPos = 0;

        System.out.println("statisticsPos --------->value " + statisticsPos);
        System.out.println();

        Map<Integer, Long> map = new HashMap<Integer, Long>();
        try {
            for (int i = 0; i < ramdomTimes; i++) {
                int[] random1 = RandomSequence.randomIntSequence(randomRange);
                int key = random1[statisticsPos];
                if (map.containsKey(key)) {
                    Long value = map.get(key);
                    map.put(key, value + 1);
                } else {
                    map.put(key, 1L);
                }
            }
            sortAndTest(map, randomRange, ramdomTimes);
        } catch (SequenceException e) {
            Assert.assertTrue(false);
        }

    }

    private ArrayList<Map.Entry<Integer, Long>> sort(Map<Integer, Long> map) {

        ArrayList<Map.Entry<Integer, Long>> list = new ArrayList<Map.Entry<Integer, Long>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, Long>>() {

            public int compare(Entry<Integer, Long> arg0, Entry<Integer, Long> arg1) {
                int result = 1;
                if (arg0.getValue() - arg1.getValue() > 0) {
                    result = -1;
                }
                return result;
            }
        });

        return list;

    }

    private void sortAndTest(Map<Integer, Long> map, long randomRange, long ramdomTimes) {
        ArrayList<Map.Entry<Integer, Long>> list = sort(map);
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<Integer, Long> entry = list.get(i);
            Long value = entry.getValue() * 100 * randomRange / ramdomTimes;
            Assert.assertTrue("POSITION " + i + " : " + value, value < MAX_PERCENTATE);
            Assert.assertTrue("POSITION " + i + " : " + value, value > MIN_PERCENTATE);
        }
    }
}
