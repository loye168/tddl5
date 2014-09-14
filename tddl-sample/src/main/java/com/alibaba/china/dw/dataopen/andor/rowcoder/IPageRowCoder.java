package com.alibaba.china.dw.dataopen.andor.rowcoder;

import com.taobao.ustore.repo.hbase.RowkeyCoderSplitImpl;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

public class IPageRowCoder extends RowkeyCoderSplitImpl {

    public IPageRowCoder(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    public String getSpliter() {
        return "\4";
    }
}
