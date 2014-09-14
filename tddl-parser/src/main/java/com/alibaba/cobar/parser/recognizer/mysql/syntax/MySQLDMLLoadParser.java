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
package com.alibaba.cobar.parser.recognizer.mysql.syntax;

import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_BY;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_INFILE;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_INTO;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_LOAD;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_OPTIONALLY;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_SET;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.KW_TABLE;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.OP_EQUALS;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.PUNC_COMMA;
import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.PUNC_RIGHT_PAREN;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLLoadStatement;
import com.alibaba.cobar.parser.recognizer.mysql.MySQLToken;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.cobar.parser.util.Pair;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class MySQLDMLLoadParser extends MySQLDMLInsertReplaceParser {

    private static enum SpecialIdentifier {
        DATA, CONCURRENT, LOCAL, COLUMNS, FIELDS
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<String, SpecialIdentifier>();
    static {
        specialIdentifiers.put("DATA", SpecialIdentifier.DATA);
        specialIdentifiers.put("CONCURRENT", SpecialIdentifier.CONCURRENT);
        specialIdentifiers.put("LOCAL", SpecialIdentifier.LOCAL);
        specialIdentifiers.put("COLUMNS", SpecialIdentifier.COLUMNS);
        specialIdentifiers.put("FIELDS", SpecialIdentifier.FIELDS);

    }

    public MySQLDMLLoadParser(MySQLLexer lexer, MySQLExprParser exprParser){
        super(lexer, exprParser);
    }

    /**
     * nothing has been pre-consumed <code><pre>
     * 'INSERT' ('LOW_PRIORITY'|'DELAYED'|'HIGH_PRIORITY')? 'IGNORE'? 'INTO'? tbname 
     *  (  'SET' colName ('='|':=') (expr|'DEFAULT') (',' colName ('='|':=') (expr|'DEFAULT'))*
     *   | '(' (  colName (',' colName)* ')' ( ('VALUES'|'VALUE') value (',' value)*
     *                                        | '(' 'SELECT' ... ')'
     *                                        | 'SELECT' ...  
     *                                       )
     *          | 'SELECT' ... ')' 
     *         )
     *   |('VALUES'|'VALUE') value  ( ',' value )*
     *   | 'SELECT' ...
     *  )
     * ( 'ON' 'DUPLICATE' 'KEY' 'UPDATE' colName ('='|':=') expr ( ',' colName ('='|':=') expr)* )?
     * 
     * value := '(' (expr|'DEFAULT') ( ',' (expr|'DEFAULT'))* ')'
     * </pre></code>
     */
    public DMLLoadStatement load() throws SQLSyntaxErrorException {
        match(KW_LOAD);
        matchIdentifier("DATA");
        DMLLoadStatement.LoadMode mode = DMLLoadStatement.LoadMode.UNDEF;
        DMLLoadStatement.DuplicateMode duplicateMode = DMLLoadStatement.DuplicateMode.UNDEF;

        String charSet = null;
        boolean local = false;
        switch (lexer.token()) {
            case KW_LOW_PRIORITY:
                lexer.nextToken();
                mode = DMLLoadStatement.LoadMode.LOW;
                break;
            case IDENTIFIER:

                if ("CONCURRENT".equals(lexer.stringValueUppercase())) {
                    mode = DMLLoadStatement.LoadMode.CONCURRENT;
                    lexer.nextToken();

                }

                break;
        }

        if ("LOCAL".equals(lexer.stringValueUppercase())) {
            local = true;
            lexer.nextToken();

        }

        match(KW_INFILE);
        LiteralString fileName = null;
        StringBuilder tempSb = null;

        switch (lexer.token()) {
            case LITERAL_CHARS:
                tempSb = new StringBuilder();
                lexer.appendStringContent(tempSb);
                fileName = new LiteralString(null, tempSb.toString(), false);
                lexer.nextToken();
                break;
            default:
                throw err("expect string after infile");
        }

        switch (lexer.token()) {
            case KW_REPLACE:
                lexer.nextToken();
                duplicateMode = DMLLoadStatement.DuplicateMode.REPLACE;
                break;
            case KW_IGNORE:
                lexer.nextToken();
                duplicateMode = DMLLoadStatement.DuplicateMode.IGNORE;
                break;
        }

        match(KW_INTO);
        match(KW_TABLE);

        Identifier table = identifier();

        switch (lexer.token()) {
            case KW_CHARACTER:
                lexer.nextToken();
                match(KW_SET);

                charSet = identifier().getIdText();
                break;
        }

        LiteralString filedsTerminatedBy = null;
        LiteralString fieldsEnclosedBy = null;
        LiteralString fieldsEscapedBy = null;
        boolean optionally = false;
        switch (lexer.token()) {
            case IDENTIFIER:

                if ("COLUMNS".equals(lexer.stringValueUppercase()) || "FIELDS".equals(lexer.stringValueUppercase())) {

                    lexer.nextToken();

                    if (lexer.token() == MySQLToken.KW_TERMINATED) {

                        lexer.nextToken();
                        match(KW_BY);

                        switch (lexer.token()) {
                            case LITERAL_CHARS:
                                tempSb = new StringBuilder();
                                lexer.appendStringContent(tempSb);
                                filedsTerminatedBy = new LiteralString(null, tempSb.toString(), false);
                                lexer.nextToken();
                                break;
                            default:
                                throw err("expect string after terminated by");
                        }

                    }

                    if (lexer.token() == KW_OPTIONALLY) {
                        optionally = true;
                        lexer.nextToken();
                        match(MySQLToken.KW_ENCLOSED);

                        match(KW_BY);

                        switch (lexer.token()) {
                            case LITERAL_CHARS:
                                tempSb = new StringBuilder();
                                lexer.appendStringContent(tempSb);
                                fieldsEnclosedBy = new LiteralString(null, tempSb.toString(), false);
                                lexer.nextToken();
                                break;
                            default:
                                throw err("expect char after enclosed by");
                        }

                    } else if (lexer.token() == MySQLToken.KW_ENCLOSED) {
                        lexer.nextToken();
                        match(KW_BY);

                        switch (lexer.token()) {
                            case LITERAL_CHARS:
                                tempSb = new StringBuilder();
                                lexer.appendStringContent(tempSb);
                                fieldsEnclosedBy = new LiteralString(null, tempSb.toString(), false);
                                lexer.nextToken();
                                break;
                            default:
                                throw err("expect char after enclosed by");
                        }

                    }
                    if (lexer.token() == MySQLToken.KW_ESCAPED) {
                        lexer.nextToken();
                        match(KW_BY);

                        switch (lexer.token()) {
                            case LITERAL_CHARS:
                                tempSb = new StringBuilder();
                                lexer.appendStringContent(tempSb);
                                fieldsEscapedBy = new LiteralString(null, tempSb.toString(), false);
                                lexer.nextToken();
                                break;
                            default:
                                throw err("expect char after escaped by");
                        }

                    }
                }
                break;
        }

        LiteralString linesStartingBy = null;
        LiteralString linesTerminatedBy = null;
        if (lexer.token() == MySQLToken.KW_LINES) {

            lexer.nextToken();

            if (lexer.token() == MySQLToken.KW_STARTING) {

                lexer.nextToken();
                match(KW_BY);

                switch (lexer.token()) {
                    case LITERAL_CHARS:
                        tempSb = new StringBuilder();
                        lexer.appendStringContent(tempSb);
                        linesStartingBy = new LiteralString(null, tempSb.toString(), false);
                        lexer.nextToken();
                        break;
                    default:
                        throw err("expect char after starting by");
                }

            }

            if (lexer.token() == MySQLToken.KW_TERMINATED) {

                lexer.nextToken();
                match(KW_BY);

                switch (lexer.token()) {
                    case LITERAL_CHARS:
                        tempSb = new StringBuilder();
                        lexer.appendStringContent(tempSb);
                        linesTerminatedBy = new LiteralString(null, tempSb.toString(), false);
                        lexer.nextToken();
                        break;
                    default:
                        throw err("expect char after terminated by");
                }

            }

        }

        Number ignoreLines = null;
        if (lexer.token() == MySQLToken.KW_IGNORE) {
            lexer.nextToken();

            switch (lexer.token()) {
                case LITERAL_NUM_PURE_DIGIT:
                    ignoreLines = lexer.integerValue();
                    lexer.nextToken();
                    break;
                default:
                    throw err("expect digit after ignore");
            }

            match(MySQLToken.KW_LINES);

        }
        List<Identifier> columnNameList = null;
        if (lexer.token() == MySQLToken.PUNC_LEFT_PAREN) {

            lexer.nextToken();

            columnNameList = idList();
            match(PUNC_RIGHT_PAREN);

        }
        List<Pair<Identifier, Expression>> values = null;
        if (lexer.token() == KW_SET) {
            lexer.nextToken();

            Identifier col = identifier();
            match(OP_EQUALS);
            Expression expr = exprParser.expression();
            if (lexer.token() == PUNC_COMMA) {
                values = new LinkedList<Pair<Identifier, Expression>>();
                values.add(new Pair<Identifier, Expression>(col, expr));
                for (; lexer.token() == PUNC_COMMA;) {
                    lexer.nextToken();
                    col = identifier();
                    match(OP_EQUALS);
                    expr = exprParser.expression();
                    values.add(new Pair<Identifier, Expression>(col, expr));
                }
            } else {
                values = new ArrayList<Pair<Identifier, Expression>>(1);
                values.add(new Pair<Identifier, Expression>(col, expr));
            }
        }

        return new DMLLoadStatement(mode,
            duplicateMode,
            charSet,
            local,
            fileName,
            table,
            filedsTerminatedBy,
            fieldsEnclosedBy,
            fieldsEscapedBy,
            optionally,
            linesStartingBy,
            linesTerminatedBy,
            ignoreLines,
            columnNameList,
            values);
    }
}
