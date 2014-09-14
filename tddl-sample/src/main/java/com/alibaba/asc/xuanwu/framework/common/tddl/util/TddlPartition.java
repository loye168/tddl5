package com.alibaba.asc.xuanwu.framework.common.tddl.util;

/**
 * 
 * 类TddlPartition.java的实现描述：
 * P4P cobar主键分库规则
 * @author liangliang.zhangll 2013-1-22 下午04:34:54
 */
public class TddlPartition {

	// 分区长度:数据段分布定义，其中取模的数一定要是2^n， 因为这里使用x % 2^n == x & (2^n - 1)等式，来优化性能。
	private static final int PARTITION_LENGTH = 1024;// 2^10

	// %转换为&操作的换算数值
	private static final long AND_VALUE = PARTITION_LENGTH - 1;
	
	/**
	 * 对long型key进行分库，返回分库id
	 * @param key 一般为客户ID
	 * @param partitionCnt 分库个数 必须为2^n值
	 * @return
	 */
	public static int partitionLong(long key, int partitionCnt){
	       int[] segment = new int[PARTITION_LENGTH];
	       if (partitionCnt <= 0 || partitionCnt > PARTITION_LENGTH
	                || PARTITION_LENGTH % partitionCnt != 0) {
	            throw new RuntimeException(
	                    "error,check your partition length definition.");
	        }

	        int index = 0;
	        int[] ai = new int[partitionCnt + 1];
	        int length = PARTITION_LENGTH / partitionCnt;
	        for (int j = 0; j < partitionCnt; j++) {
	            ai[++index] = ai[index - 1] + length;
	        }
	        if (ai[ai.length - 1] != PARTITION_LENGTH) {
	            throw new RuntimeException(
	                    "error,check your partition length definition.");
	        }
	        // 数据映射操作
	        for (int i = 1; i < ai.length; i++) {
	            for (int j = ai[i - 1]; j < ai[i]; j++) {
	                segment[j] = (i - 1);
	            }
	        }
	        return (segment[(int) (key & AND_VALUE)] + 1);
	}
	
	public static void main(String[] args){

	    System.out.println(TddlPartition.partitionLong(123456789L, 4));
	}
}
