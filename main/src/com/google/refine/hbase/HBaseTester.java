package com.google.refine.hbase;

import java.io.IOException;

import org.json.JSONException;
import com.google.refine.hbase.HBaseWriterStream;

public class HBaseTester {

    public static void main(String[] args) throws IOException {
        HBaseProjectDataWriterStream ostream = new HBaseProjectDataWriterStream("2277742892131");
        
        
        
//        HBaseReader reader = new HBaseReader("2277742892131.project");
//        try {
//            System.out.println(reader.getMetadataObj());
//        } catch (JSONException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

    }

}
