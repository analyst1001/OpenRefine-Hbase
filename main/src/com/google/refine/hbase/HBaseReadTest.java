package com.google.refine.hbase;

import java.io.IOException;
import com.google.refine.hbase.HBaseReader;


public class HBaseReadTest {
    public static void main(String args[]) throws IOException {
        HBaseReader reader = new HBaseReader("2277742892131.project");
        reader.getRow();
        System.out.println("Done");
    }
}
