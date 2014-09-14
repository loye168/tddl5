package com.taobao.tddl.client.sequence.impl;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.client.sequence.SequenceDao;
import com.taobao.tddl.client.sequence.SequenceRange;
import com.taobao.tddl.client.sequence.exception.SequenceException;

public class DefaultSequenceDaoTest {

    private ApplicationContext context;
    private SequenceDao        sequenceDao;

    @Before
    public void setUp() throws Exception {
        context = new ClassPathXmlApplicationContext(new String[] { "classpath:spring-context-old.xml" });
        sequenceDao = (SequenceDao) context.getBean("sequenceDao");
    }

    @Ignore
    @Test
    public void test_nextRange() {
        try {
            SequenceRange sequenceRange = sequenceDao.nextRange("xdh");
            System.out.println(sequenceRange);
        } catch (SequenceException e) {
            e.printStackTrace();
        }
    }

}
