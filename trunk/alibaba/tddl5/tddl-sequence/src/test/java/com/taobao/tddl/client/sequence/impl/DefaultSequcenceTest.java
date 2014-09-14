package com.taobao.tddl.client.sequence.impl;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.exception.SequenceException;

public class DefaultSequcenceTest {

    private ApplicationContext context;
    private Sequence           sequence;

    @Before
    public void setUp() throws Exception {
        context = new ClassPathXmlApplicationContext(new String[] { "classpath:spring-context-old.xml" });
        sequence = (Sequence) context.getBean("sequence");
    }

    @Ignore
    @Test
    public void test_nextValue() throws SequenceException {
        Set<Long> set = new HashSet<Long>();
        for (int i = 0; i < 1000; i++) {
            Long id = sequence.nextValue();
            System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }
    }
}
