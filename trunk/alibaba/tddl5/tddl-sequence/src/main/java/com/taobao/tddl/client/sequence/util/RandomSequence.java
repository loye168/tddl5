package com.taobao.tddl.client.sequence.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.taobao.tddl.client.sequence.exception.SequenceException;

public class RandomSequence {

    /**
     * 产生包含0~n-1的n个数值的随机序列
     * 
     * @param n
     * @return
     * @throws SequenceException
     */
    public static int[] randomIntSequence(int n) throws SequenceException {
        if (n <= 0) {
            throw new SequenceException("产生随机序列范围值小于等于0");
        }
        int num[] = new int[n];
        for (int i = 0; i < n; i++) {
            num[i] = i;
        }
        if (n == 1) {
            return num;
        }
        Random random = new Random();
        if (n == 2 && random.nextInt(2) == 1) // 50%的概率换一下
        {
            int temp = num[0];
            num[0] = num[1];
            num[1] = temp;
        }

        // for (int i = 0; i < n + 10; i++) {
        // int rindex = random.nextInt(n);// 产生0~n-1的随机数
        // int mindex = random.nextInt(n);
        // int temp = num[mindex];
        // num[mindex] = num[rindex];
        // num[rindex] = temp;
        // }
        // return num;

        int[] result = randomIntSequence(num);

        return result;
    }

    /**
     * 乱序一个数组
     * 
     * @param sourceQueue
     * @return
     * @throws SequenceException
     */
    public static int[] randomIntSequence(int[] sourceQueue) {
        int size = sourceQueue.length;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>(size);
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int randomNum = random.nextInt(size * 100);
            map.put(sourceQueue[i], randomNum);
        }
        ArrayList<Map.Entry<Integer, Integer>> resultQueue = sortByValueAsc(map);
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultQueue.get(i).getKey();
        }
        return result;
    }

    private static ArrayList<Map.Entry<Integer, Integer>> sortByValueAsc(Map<Integer, Integer> map) {
        ArrayList<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {

            public int compare(Entry<Integer, Integer> arg0, Entry<Integer, Integer> arg1) {
                int result = -1;
                if (arg0.getValue() - arg1.getValue() > 0) {
                    result = 1;
                }
                return result;
            }
        });
        return list;
    }
}
