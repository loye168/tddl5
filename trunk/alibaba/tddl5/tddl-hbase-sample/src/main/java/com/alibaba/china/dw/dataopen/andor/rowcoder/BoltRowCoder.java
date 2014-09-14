package com.alibaba.china.dw.dataopen.andor.rowcoder;

import com.taobao.ustore.repo.hbase.RowkeyCoderSplitImpl;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

public class BoltRowCoder extends RowkeyCoderSplitImpl {

    public BoltRowCoder(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    public String getSpliter() {
        return " ";
    }

}
