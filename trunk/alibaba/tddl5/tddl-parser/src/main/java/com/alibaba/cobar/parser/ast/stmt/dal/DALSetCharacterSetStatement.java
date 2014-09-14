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
 * (created at 2011-9-23)
 */
package com.alibaba.cobar.parser.ast.stmt.dal;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DALSetCharacterSetStatement implements SQLStatement {

    private final String charset;

    public DALSetCharacterSetStatement(){
        this.charset = null;
    }

    public DALSetCharacterSetStatement(String charset){
        if (charset == null) throw new IllegalArgumentException("charsetName is null");
        this.charset = charset;
    }

    public boolean isDefault() {
        return charset == null;
    }

    public String getCharset() {
        return charset;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
