package com.taobao.tddl.statistics;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.record.CloneableRecord;

public class FetchFromLeftOperation extends AbstractSQLOperation {

    List<CloneableRecord>             left;
    List<String>                      columns = new ArrayList(0);
    public final static MessageFormat message = new MessageFormat("Get from left: {0}={1}");

    public FetchFromLeftOperation(List<CloneableRecord> left){
        super();
        this.left = left;

        if (!left.isEmpty()) {
            CloneableRecord record = left.get(0);
            columns = record.getColumnList();

        }
    }

    @Override
    public String getOperationString() {

        StringBuilder columnsStr = new StringBuilder();
        if (columns.size() == 1) {
            columnsStr.append(columns.get(0));
        } else {
            columnsStr.append("(");

            boolean first = true;

            for (String column : columns) {
                if (first) {
                    first = false;
                } else {
                    columnsStr.append(",");
                }

                columnsStr.append(column);
            }
            columnsStr.append(")");
        }

        StringBuilder valuesStr = new StringBuilder();

        boolean firstValue = true;

        for (CloneableRecord record : left) {
            if (firstValue) {
                firstValue = false;
            } else {
                valuesStr.append(",");
            }

            valuesStr.append(getPair(record));
        }
        return message.format(new String[] { columnsStr.toString(), valuesStr.toString() });
    }

    public String getPair(CloneableRecord record) {

        if (columns.size() == 1) {
            return String.valueOf(record.get(columns.get(0)));
        }
        boolean first = true;

        StringBuilder b = new StringBuilder();

        b.append("(");

        for (String column : columns) {
            if (first) {
                first = false;
            } else {
                b.append(",");
            }

            b.append(record.get(column));
        }
        b.append(")");

        return b.toString();

    }

    @Override
    public String getOperationType() {
        return "Fetch From Left";
    }

    @Override
    public String getSqlOrResult() {
        return null;
    }

}
