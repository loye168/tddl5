/**
 * (created at 2011-8-5)
 */
package com.alibaba.cobar.hint;

import junit.framework.TestCase;

import org.junit.Assert;

import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.server.ugly.hint.CobarHint;
import com.alibaba.cobar.server.ugly.hint.HintRouter;
import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.optimizer.parse.hint.SimpleHintParser;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class CobarHintTest extends TestCase {

    public void testHint1() throws Exception {
        String sql = "  /*!drds: $dataNodeId =2.1, $table='offer'*/ select * ";
        CobarHint hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals(1, hint.getDataNodes().size());
        Assert.assertEquals(new Pair<Integer, Integer>(2, 1), hint.getDataNodes().get(0));
        sql = HintRouter.convertHint(sql);
        Assert.assertEquals("/*+TDDL({\"dbid\":\"2\",\"dbindex\":true,\"type\":\"direct\",\"vtab\":\"OFFER\"})*/ select * ", sql);

        sql = "  /*!drds: $dataNodeId=0.0, $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals(1, hint.getDataNodes().size());
        Assert.assertEquals(new Pair<Integer, Integer>(0, 0), hint.getDataNodes().get(0));
        sql = HintRouter.convertHint(sql);

        sql = "  /*!drds: $dataNodeId=0, $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals(1, hint.getDataNodes().size());
        Assert.assertEquals(new Pair<Integer, Integer>(0, null), hint.getDataNodes().get(0));
        sql = HintRouter.convertHint(sql);

        sql = "/*!drds: $partitionOperand=( 'member_id' = 'm1'), $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Pair<String[], Object[][]> pair = hint.getPartitionOperand();
        Assert.assertEquals(1, pair.getKey().length);
        Assert.assertEquals("MEMBER_ID", pair.getKey()[0]);
        Assert.assertEquals(1, pair.getValue().length);
        Assert.assertEquals(1, pair.getValue()[0].length);
        Assert.assertEquals("m1", pair.getValue()[0][0]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        RouteCondition rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand =   ( 'member_id' = ['m1'  ,   'm2' ]  ), $table='offer'  , $replica=  2*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals(2, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(1, pair.getKey().length);
        Assert.assertEquals("MEMBER_ID", pair.getKey()[0]);
        Assert.assertEquals(2, pair.getValue().length);
        Assert.assertEquals(1, pair.getValue()[0].length);
        Assert.assertEquals("m1", pair.getValue()[0][0]);
        Assert.assertEquals("m2", pair.getValue()[1][0]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand=('member_id'=['m1', 'm2']),$table='offer',$replica=2*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals(2, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(1, pair.getKey().length);
        Assert.assertEquals("MEMBER_ID", pair.getKey()[0]);
        Assert.assertEquals(2, pair.getValue().length);
        Assert.assertEquals(1, pair.getValue()[0].length);
        Assert.assertEquals("m1", pair.getValue()[0][0]);
        Assert.assertEquals("m2", pair.getValue()[1][0]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand = ( ['offer_id','group_id'] = [123,'3c']), $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(2, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals("GROUP_ID", pair.getKey()[1]);
        Assert.assertEquals(1, pair.getValue().length);
        Assert.assertEquals(2, pair.getValue()[0].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals("3c", pair.getValue()[0][1]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand=(['offer_id' , 'group_iD' ]=[ 123 , '3c' ]) ,$table = 'offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(2, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals("GROUP_ID", pair.getKey()[1]);
        Assert.assertEquals(1, pair.getValue().length);
        Assert.assertEquals(2, pair.getValue()[0].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals("3c", pair.getValue()[0][1]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand=(['offer_id','group_id']=[123,'3c']),$table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(2, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals("GROUP_ID", pair.getKey()[1]);
        Assert.assertEquals(1, pair.getValue().length);
        Assert.assertEquals(2, pair.getValue()[0].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals("3c", pair.getValue()[0][1]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand=(['offer_id','group_id']=[[123,'3c'],[234,'food']]), $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(2, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals("GROUP_ID", pair.getKey()[1]);
        Assert.assertEquals(2, pair.getValue().length);
        Assert.assertEquals(2, pair.getValue()[0].length);
        Assert.assertEquals(2, pair.getValue()[1].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals("3c", pair.getValue()[0][1]);
        Assert.assertEquals(234L, pair.getValue()[1][0]);
        Assert.assertEquals("food", pair.getValue()[1][1]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand= ( [ 'ofFER_id','groUp_id' ]= [ [123,'3c'],[ 234,'food']]  ), $table='offer'*/select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals("select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(2, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals("GROUP_ID", pair.getKey()[1]);
        Assert.assertEquals(2, pair.getValue().length);
        Assert.assertEquals(2, pair.getValue()[0].length);
        Assert.assertEquals(2, pair.getValue()[1].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals("3c", pair.getValue()[0][1]);
        Assert.assertEquals(234L, pair.getValue()[1][0]);
        Assert.assertEquals("food", pair.getValue()[1][1]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);

        sql = "/*!drds:$partitionOperand=(['offer_id']=[123,234]), $table='offer'*/ select * ";
        hint = CobarHint.parserCobarHint(sql, HintRouter.indexOfPrefix(sql));
        Assert.assertEquals(" select * ", hint.getOutputSql());
        Assert.assertEquals("OFFER", hint.getTable());
        Assert.assertEquals((int) CobarHint.DEFAULT_REPLICA_INDEX, hint.getReplica());
        pair = hint.getPartitionOperand();
        Assert.assertEquals(1, pair.getKey().length);
        Assert.assertEquals("OFFER_ID", pair.getKey()[0]);
        Assert.assertEquals(2, pair.getValue().length);
        Assert.assertEquals(1, pair.getValue()[0].length);
        Assert.assertEquals(1, pair.getValue()[1].length);
        Assert.assertEquals(123L, pair.getValue()[0][0]);
        Assert.assertEquals(234L, pair.getValue()[1][0]);
        Assert.assertNull(hint.getDataNodes());
        sql = HintRouter.convertHint(sql);
        rc = SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(rc);
    }
}
