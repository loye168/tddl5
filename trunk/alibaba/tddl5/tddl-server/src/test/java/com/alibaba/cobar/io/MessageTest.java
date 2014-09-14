package com.alibaba.cobar.io;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.cobar.net.util.MySQLMessage;

/**
 * @author xianmao.hexm
 */
public class MessageTest {

    @Test
    public void testReadBytesWithNull() {
        byte[] bytes = new byte[] { 1, 2, 3, 0, 5 };
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(3, ab.length);
        Assert.assertEquals(4, message.position());
    }

    @Test
    public void testReadBytesWithNull2() {
        byte[] bytes = new byte[] { 0, 1, 2, 3, 0, 5 };
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(1, message.position());
    }

    @Test
    public void testReadBytesWithNull3() {
        byte[] bytes = new byte[] {};
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(0, message.position());
    }

}
