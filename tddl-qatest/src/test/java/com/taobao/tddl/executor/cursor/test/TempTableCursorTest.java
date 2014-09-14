package com.taobao.tddl.executor.cursor.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.MockArrayCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.cursor.impl.TempTableCursor;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class TempTableCursorTest {

    MockArrayCursor getCursor(String tableName, Integer[] ids) throws TddlException {
        MockArrayCursor cursor = new MockArrayCursor(tableName);
        cursor.addColumn("id", DataType.IntegerType);
        cursor.addColumn("name", DataType.StringType);
        cursor.addColumn("school", DataType.StringType);
        cursor.initMeta();

        for (Integer id : ids) {
            cursor.addRow(new Object[] { id, "name" + id, "school" + id });

        }

        cursor.init();

        return cursor;

    }

    static ExecutionContext ec = new ExecutionContext();

    @Test
    public void testWithOutColumns() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        TempTableCursor c = new TempTableCursor(bdbRepo, subCursor, true, 1, ec);
        Object[] expected = new Object[] { 1, 1, 2, 3, 4, 5, 5 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor.isClosed());
    }

    @Test
    public void testWithColumns() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        ColumnMeta cm = new ColumnMeta("T1", "NAME", DataType.IntegerType, null, true);
        TempTableCursor c = new TempTableCursor(bdbRepo, subCursor, true, Arrays.asList(cm), 2, ec);
        Object[] expected = new Object[] { "name1", "name2", "name3", "name4", "name5", "name5" };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor.isClosed());
    }

    @Test
    public void testWithOutColumnsBeforeFirst() throws TddlException {
        RepositoryHolder repoHolder = new RepositoryHolder();
        StaticSchemaManager sm = new StaticSchemaManager("test_schema.xml", null, null);
        sm.init();
        IRepository bdbRepo = repoHolder.getOrCreateRepository("BDB_JE", Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        MockArrayCursor mockCursor = this.getCursor("T1", new Integer[] { 5, 5, 4, 3, 2, 1 });
        SchematicCursor subCursor = new SchematicCursor(mockCursor);

        TempTableCursor c = new TempTableCursor(bdbRepo, subCursor, true, 3, ec);
        Object[] expected = new Object[] { 1, 2, 3, 4, 5, 5, 1, 2, 3, 4, 5, 5 };
        List actual = new ArrayList();

        IRowSet row = null;
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }

        c.beforeFirst();
        while ((row = c.next()) != null) {

            System.out.println(row);
            actual.add(row.getObject(0));
        }
        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor.isClosed());
    }

}
