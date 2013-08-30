package com.google.refine.hbase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.refine.util.JSONUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseWriterStream extends OutputStream {

    final static int MAX_LENGTH = 10000;
    String buffer;
    int currIndex;
    private static Configuration conf = null;
    private static String tableName = null;
    String rowKey = null;
    //Constructor
    public HBaseWriterStream(String projectID) {
        buffer = new String();
        currIndex = 0;
        conf = (new HBaseConfiguration()).create();
        tableName = new String("metadata");
        rowKey = projectID;
    }
    
    public void insertRecord(String tableName, String rowKey, String colFamily, String colName, String value) throws IOException {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(value));
            table.put(put);
         } catch (IOException e) {
                e.printStackTrace();
         }      
    }
    
    public void insertRecord(String tableName, String rowKey, String colFamily, String colName, int value) throws IOException {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(value));
            table.put(put);
         } catch (IOException e) {
                e.printStackTrace();
         }      
    }
    
    public void saveFromJSON(JSONObject obj) throws IOException {
        System.out.println("Saving from JSON");
        Date _created = JSONUtilities.getDate(obj, "created", new Date());
        insertRecord(tableName, rowKey, "created", "created", String.format("%tFT%<tTZ", _created));
        Date _modified = JSONUtilities.getDate(obj, "modified", new Date());
        insertRecord(tableName, rowKey, "modified", "modified", String.format("%tFT%<tTZ", _modified));
        String _name = JSONUtilities.getString(obj, "name", "<Error recovering project name>");
        insertRecord(tableName, rowKey, "name", "name", _name.toString());
        String _password = JSONUtilities.getString(obj, "password", "");
        insertRecord(tableName, rowKey, "password", "password", _password.toString());
        String _encoding = JSONUtilities.getString(obj, "encoding", "");
        insertRecord(tableName, rowKey, "encoding", "encoding", _encoding.toString());
        int _encodingConfidence = JSONUtilities.getInt(obj, "encodingConfidence", 0);
        insertRecord(tableName, rowKey, "encodingConfidence", "encodingConfidence", _encodingConfidence);
        System.out.println("Going to write preferences");
        if (obj.has("preferences") && !obj.isNull("preferences")) {
            try {
                System.out.println("Inside preferences");
                JSONObject obj2 = obj.getJSONObject("preferences");
                if (obj2.has("entries") && !obj2.isNull("entries")) {
                    System.out.println("Inside entries");
                    JSONObject entries = obj2.getJSONObject("entries");
                    
                    @SuppressWarnings("unchecked")
                    Iterator<String> i = entries.keys();
                    while (i.hasNext()) {
                        String key = i.next();
                        if (!entries.isNull(key)) {
                            Object o = entries.get(key);
                            System.out.println("Preferences " + key + " : " + o.toString());
                            insertRecord(tableName, rowKey, "preferences", key, o.toString());
                        }
                    }
                }
            } catch (JSONException e) {
                // ignore
            }
        }
        System.out.println("Going to write custom metadata");
        if (obj.has("customMetadata") && !obj.isNull("customMetadata")) {
            try {
                JSONObject obj2 = obj.getJSONObject("customMetadata");
                
                @SuppressWarnings("unchecked")
                Iterator<String> keys = obj2.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = obj2.get(key);
                    if (value != null && value instanceof Serializable) {
                        System.out.println("customMetadata " + key + " : " + value.toString());
                        insertRecord(tableName, rowKey, "customMetadata", key, value.toString());
                    }
                }
            } catch (JSONException e) {
                // ignore
            }
        }
    }
    
    public void parseAndExecute() {
        JSONTokener tokener = new JSONTokener(buffer);
        try {
            JSONObject obj = (JSONObject) tokener.nextValue();

                saveFromJSON(obj);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }
    
    @Override
    public void flush() {
        parseAndExecute();
        currIndex = 0;
    }
    
    @Override
    public void write(int b)
            throws IOException {
        System.out.println("wrinting " + (char) b);
        buffer = buffer + (char) b;
    }
    
    @Override
    public void close() throws IOException {
        flush();
        super.close();
    }

}
