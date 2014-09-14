//package com.alibaba.cobar.util;
//
//import java.util.Map;
//import java.util.Set;
//
//import junit.framework.TestCase;
//
//import com.alibaba.cobar.server.util.MetaUtil;
//
//public class MetaUtilTest extends TestCase {
//
//    public void testMetaUtil() {
//        Map<String, Map<String, Set<String>>> appTopo = MetaUtil.getAppTopo("ACSS_BILL_APP");
//        for(String logicTable: appTopo.keySet()) {
//            System.out.println(logicTable);
//            System.out.println(appTopo.get(logicTable));
//        }
//    }
//    
//}
