/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-5-18)
 */
package com.alibaba.cobar.parser.ast.stmt.dml;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DMLLoadStatement extends DMLStatement {

    public static enum LoadMode {
        /** default */
        UNDEF, LOW, CONCURRENT
    }

    public static enum DuplicateMode {
        /** default */
        UNDEF, REPLACE, IGNORE
    }

    private LoadMode                           mode;
    private DuplicateMode                      duplicateMode;
    private String                             charSet;
    private boolean                            local;
    private LiteralString                      fileName;
    private Identifier                         table;
    private LiteralString                      filedsTerminatedBy;
    private LiteralString                      fieldsEnclosedBy;
    private LiteralString                      fieldsEscapedBy;
    private boolean                            optionally;
    private LiteralString                      linesStartingBy;
    private LiteralString                      linesTerminatedBy;
    private Number                             ignoreLines;
    private List<Identifier>                   columnNameList;
    private List<Pair<Identifier, Expression>> values;

    public DMLLoadStatement(LoadMode mode, DuplicateMode duplicateMode, String charSet, boolean local,
                            LiteralString fileName, Identifier table, LiteralString filedsTerminatedBy,
                            LiteralString fieldsEnclosedBy, LiteralString fieldsEscapedBy, boolean optionally,
                            LiteralString linesStartingBy, LiteralString linesTerminatedBy, Number ignoreLines,
                            List<Identifier> columnNameList, List<Pair<Identifier, Expression>> values){
        this.mode = mode;
        this.duplicateMode = duplicateMode;
        this.charSet = charSet;
        this.local = local;
        this.fileName = fileName;
        this.table = table;
        this.filedsTerminatedBy = filedsTerminatedBy;
        this.fieldsEnclosedBy = fieldsEnclosedBy;
        this.fieldsEscapedBy = fieldsEscapedBy;

        this.optionally = optionally;
        this.linesStartingBy = linesStartingBy;
        this.linesTerminatedBy = linesTerminatedBy;

        this.ignoreLines = ignoreLines;

        this.columnNameList = columnNameList;

        this.values = values;

    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public LoadMode getMode() {
        return mode;
    }

    public void setMode(LoadMode mode) {
        this.mode = mode;
    }

    public DuplicateMode getDuplicateMode() {
        return duplicateMode;
    }

    public void setDuplicateMode(DuplicateMode duplicateMode) {
        this.duplicateMode = duplicateMode;
    }

    public String getCharSet() {
        return charSet;
    }

    public void setCharSet(String charSet) {
        this.charSet = charSet;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public LiteralString getFileName() {
        return fileName;
    }

    public void setFileName(LiteralString fileName) {
        this.fileName = fileName;
    }

    public Identifier getTable() {
        return table;
    }

    public void setTable(Identifier table) {
        this.table = table;
    }

    public LiteralString getFiledsTerminatedBy() {
        return filedsTerminatedBy;
    }

    public void setFiledsTerminatedBy(LiteralString filedsTerminatedBy) {
        this.filedsTerminatedBy = filedsTerminatedBy;
    }

    public LiteralString getFieldsEnclosedBy() {
        return fieldsEnclosedBy;
    }

    public void setFieldsEnclosedBy(LiteralString fieldsEnclosedBy) {
        this.fieldsEnclosedBy = fieldsEnclosedBy;
    }

    public LiteralString getFieldsEscapedBy() {
        return fieldsEscapedBy;
    }

    public void setFieldsEscapedBy(LiteralString fieldsEscapedBy) {
        this.fieldsEscapedBy = fieldsEscapedBy;
    }

    public boolean isOptionally() {
        return optionally;
    }

    public void setOptionally(boolean optionally) {
        this.optionally = optionally;
    }

    public LiteralString getLinesStartingBy() {
        return linesStartingBy;
    }

    public void setLinesStartingBy(LiteralString linesStartingBy) {
        this.linesStartingBy = linesStartingBy;
    }

    public LiteralString getLinesTerminatedBy() {
        return linesTerminatedBy;
    }

    public void setLinesTerminatedBy(LiteralString linesTerminatedBy) {
        this.linesTerminatedBy = linesTerminatedBy;
    }

    public Number getIgnoreLines() {
        return ignoreLines;
    }

    public void setIgnoreLines(Number ignoreLines) {
        this.ignoreLines = ignoreLines;
    }

    public List<Identifier> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<Identifier> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public List<Pair<Identifier, Expression>> getValues() {
        return values;
    }

    public void setValues(List<Pair<Identifier, Expression>> values) {
        this.values = values;
    }

}
