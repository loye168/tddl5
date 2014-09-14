package com.taobao.tddl.client.user.method;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class B2BDWCBUPartitionMethod {

    private static final int DEFAULT_STR_HEAD_LEN = 8;

    public static int partitionString(String key, int partitionCnt) {
        long h = 0;
        int i = 0;
        while (i < DEFAULT_STR_HEAD_LEN && i < key.length()) {
            h = (h << 5) - h + key.charAt(i);
            i++;
        }
        return (int)(Math.abs(h) % partitionCnt);
    }
    
    public static int partitionString(Integer key, int partitionCnt) {
    	String strKey = String.valueOf(key);
    	long h = 0;
        int i = 0;
        while (i < DEFAULT_STR_HEAD_LEN && i < strKey.length()) {
            h = (h << 5) - h + strKey.charAt(i);
            i++;
        }
        return (int)(Math.abs(h) % partitionCnt);
    }
    
    public static int partitionLong(long val, int partitionCnt) {
        
        return (int) (Math.abs(val) % partitionCnt);
    }
    
    public static int partitionLong(String val, int partitionCnt) {
        
        return (int) (Math.abs(Integer.valueOf(val)) % partitionCnt);
    }
    
    public static int partitionDate(Date val, int partitionCnt) throws ParseException {
    	Date start =  new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
    	
        return (int)((val.getTime() - start.getTime()) / (24 * 60 * 60 * 1000)) % partitionCnt;
    }
    
    public static int partitionDate(String val, int partitionCnt) throws ParseException{
    	Date dt =  new SimpleDateFormat("yyyyMMdd").parse(val);
    	Date start =  new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
    	
        return (int)((dt.getTime() - start.getTime()) / (24 * 60 * 60 * 1000)) % partitionCnt;
    }
}
