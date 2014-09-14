package com.alibaba.cobar.parser.recognizer.mysql.syntax;

import static com.alibaba.cobar.parser.recognizer.mysql.MySQLToken.EOF;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.reload.ReloadSchema;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;

/**
 * @author mengshi.sunmengshi 2014年6月3日 下午4:13:52
 * @since 5.1.0
 */
public class ReloadParser extends MySQLParser {

    private static enum SpecialIdentifier {
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<String, SpecialIdentifier>();
    static {
        // specialIdentifiers.put("SAVEPOINT", SpecialIdentifier.SAVEPOINT);
    }

    public ReloadParser(MySQLLexer lexer){
        super(lexer);
    }

    /**
     * reload schema
     * 
     * @return
     * @throws SQLSyntaxErrorException
     */
    public SQLStatement reload() throws SQLSyntaxErrorException {
        lexer.nextToken();

        switch (lexer.token()) {
        // case EOF:
        // return new
        // MTSRollbackStatement(MTSRollbackStatement.CompleteType.UN_DEF);
            case KW_SCHEMA:
                lexer.nextToken();

                match(EOF);
                return new ReloadSchema();

            default:
                throw err("unrecognized complete type: " + lexer.token());
        }
    }

}
