package com.taobao.tddl.repo.demo.cursor;

import java.util.List;

import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.record.CloneableRecord;

public class DemoUtil {

    public static Object getValueOfKey(CloneableRecord key) {
        List<Object> valColumnList = key.getValueList();
        if (valColumnList.size() != 1) {
            throw new UnsupportedOperationException("demo只支持单值查询，" + "多值需要复写comparable接口来做比较哦，亲，这需要你来实现:)");
        }
        Object valOfKeys = valColumnList.get(0);
        return valOfKeys;
    }

    public static CloneableRecord getCloneableRecordOfKey(String keyCol, Object keyVal, RecordCodec keyCodec) {
        CloneableRecord record = keyCodec.newEmptyRecord();
        record.put(keyCol, keyVal);
        return record;
    }
}
