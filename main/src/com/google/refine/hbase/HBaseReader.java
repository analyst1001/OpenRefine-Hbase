package com.google.refine.hbase;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseReader {

    private String tableName = null;
    private String rowKey;
    private static Configuration conf = null;
    
    public HBaseReader(String projectID) {
        tableName = new String("metadata");
        rowKey = projectID;
        conf = (new HBaseConfiguration()).create();
    }
    
    public Result getRow() throws IOException {
        HTable table = new HTable(conf, tableName);
        Filter filter = new PrefixFilter(Bytes.toBytes(rowKey));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Result res = scanner.next();
        return res;
    }
    
    public JSONObject getMetadataObj() throws JSONException {
        JSONObject obj = new JSONObject();
        try {
            Result res;
            res = getRow();
            for (KeyValue kv : res.raw()) {
                if (Bytes.toString(kv.getFamily()).compareTo("encodingConfidence") == 0) {
                    obj.put("encodingConfidence", Bytes.toInt(kv.getValue()));
                }
                else if (Bytes.toString(kv.getFamily()).compareTo("preferences") == 0) {
                    if (!obj.has("preferences")) {
                        JSONObject obj2 = new JSONObject();
                        obj2.put("entries", new JSONObject());
                        obj.put("preferences", obj2);
                    }
                    obj.getJSONObject("preferences").getJSONObject("entries").put(Bytes.toString(kv.getQualifier()), (new JSONTokener(Bytes.toString(kv.getValue())).nextValue()));                        
                }
                else if (Bytes.toString(kv.getFamily()).compareTo("customMetadata") == 0) {
                    obj.getJSONObject("customMetadata").put(Bytes.toString(kv.getQualifier()), (new JSONTokener(Bytes.toString(kv.getValue())).nextValue()));
                }
                else if (Bytes.toString(kv.getFamily()).compareTo("created") == 0) {
                    obj.put(Bytes.toString(kv.getFamily()), Bytes.toString(kv.getValue()));
                }
                else if (Bytes.toString(kv.getFamily()).compareTo("modified") == 0) {
                    obj.put(Bytes.toString(kv.getFamily()), Bytes.toString(kv.getValue()));
                }
                else  {
                    obj.put(Bytes.toString(kv.getFamily()), (new JSONTokener(Bytes.toString(kv.getValue())).nextValue()));
                }
              }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return obj;
    }
}
