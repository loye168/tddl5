package com.alibaba.cobar.io;

import java.nio.ByteBuffer;

/**
 * @author xianmao.hexm 2011-5-9
 */
public class BufferPerformanceMain {

    public void getAllocate() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Get:allocate)");
    }

    public void getAllocateDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Get:allocateDirect)");
    }

    public void putAllocate() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.put(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Put:allocate)");
    }

    public void putAllocateDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        byte[] b = new byte[1024];

        int count = 1000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.put(b);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(Put:allocateDirect)");
    }

    public void copyArrayDirect() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        while (buffer.hasRemaining()) {
            buffer.put((byte) 1);
        }
        byte[] b = new byte[1024];
        int count = 10000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b, 0, b.length);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(testCopyArrayDirect)");
    }

    public void copyArray() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        while (buffer.hasRemaining()) {
            buffer.put((byte) 1);
        }
        byte[] b = new byte[1024];
        int count = 10000000;
        System.currentTimeMillis();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            buffer.position(0);
            buffer.get(b, 0, b.length);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take time:" + (t2 - t1) + " ms.(testCopyArray)");
    }

    public static void main(String[] args) {

    }

}
