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
 * (created at 2011-7-5)
 */
package com.alibaba.cobar.parser.ast.stmt.ddl;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;

/**
 * @author mengshi.sunmengshi 2014年5月6日 下午4:47:03
 * @since 5.1.0
 */
public class CreateSequence implements DDLStatement {

    private Number     increment;
    private Identifier name;
    private Number     batch;
    private Number     start;

    public CreateSequence(Identifier name, Number batch2, Number increment2, Number start2){
        this.name = name;
        this.batch = batch2;
        this.increment = increment2;
        this.start = start2;

    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public Number getIncrement() {
        return increment;
    }

    public void setIncrement(Number increment) {
        this.increment = increment;
    }

    public Identifier getName() {
        return name;
    }

    public void setName(Identifier name) {
        this.name = name;
    }

    public Number getBatch() {
        return batch;
    }

    public void setBatch(Number batch) {
        this.batch = batch;
    }

    public Number getStart() {
        return start;
    }

    public void setStart(Number start) {
        this.start = start;
    }

}
