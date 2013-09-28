package com.google.refine.hbase;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.util.JSONUtilities;


public class HBaseProjectDataWriterStream extends OutputStream {

    String buffer;
    private static Configuration conf = null;
    String projectName = null;
    
    public enum PSTATE {VERSION, COLUMN, COLUMNGROUP, HISTORY, ROW, COMPLETED};
    
    PSTATE state = PSTATE.VERSION;
    
    String versionStr, columnModel, maxCellIndexStr, keyColumnIndexStr, columnCountStr, columnGroupCountStr, historyStr, pastEntryCountStr, futureEntryCountStr, rowCountStr;
    int maxCellIndex, keyColumnIndex, columnCount, currentColumnsRead, columnGroupCount, rowCount, currentRowsRead, pastEntryCount,currentPastEntryCount;
    String[] columnNames = null;
    
    
    public HBaseProjectDataWriterStream(String projName) {
        buffer = new String();
        conf = (new HBaseConfiguration()).create();
        projectName = projName;
        columnCount = -1;
        currentColumnsRead = 0;
        rowCount = -1;
        currentRowsRead = 0;
        pastEntryCount = -1;
        currentPastEntryCount = 0;
    }
    
    public void createTable() {
        //logger.info("****************************************Creating Table");
        HBaseAdmin hbase;
        try {
            hbase = new HBaseAdmin(conf);
            HTableDescriptor desc = new HTableDescriptor(projectName);
            HColumnDescriptor starredCol = new HColumnDescriptor("starred".getBytes());
            desc.addFamily(starredCol);
            HColumnDescriptor flaggedCol = new HColumnDescriptor("flagged".getBytes());
            desc.addFamily(flaggedCol);
            for (int i = 0; i < columnCount; i++) {
                if (i != keyColumnIndex) {
                    HColumnDescriptor col = new HColumnDescriptor(columnNames[i].getBytes());
                    desc.addFamily(col);
                }
            }
            hbase.createTable(desc);
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("Error occurred while creating TABLE");
            e.printStackTrace();
        }
        
    }
    
    final static Logger logger = LoggerFactory.getLogger("project_utilities");
    void insertRecord(JSONObject obj) {
        //logger.info("****************************************Inserting Row");
        try {
            HTable table = new HTable(conf, projectName);
            Put put = new Put(Bytes.toBytes(obj.getJSONArray("cells").getJSONObject(keyColumnIndex).getString("v")));
            put.add(Bytes.toBytes("starred"), Bytes.toBytes("starred"), Bytes.toBytes(obj.getString("starred")));
            put.add(Bytes.toBytes("flagged"), Bytes.toBytes("flagged"), Bytes.toBytes(obj.getString("flagged")));
            for (int i = 0; i < columnCount; i++) {
                if (i != keyColumnIndex) {
                    try {
                        put.add(Bytes.toBytes(columnNames[i]), Bytes.toBytes(columnNames[i]), Bytes.toBytes(obj.getJSONArray("cells").getJSONObject(i).getString("v")));
                    }
                    catch (JSONException e) {
                        put.add(Bytes.toBytes(columnNames[i]), Bytes.toBytes(columnNames[i]), null);

                    }
                    table.put(put);
                } 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    public void parseAndExecute() {
        boolean nothingDone = false;
        while (!nothingDone) {
            nothingDone = true;
            //logger.info("Inside while loop");
            switch (state) {
                case VERSION:
                    //logger.info("Inside Version");
                    versionStr = buffer.split("\n")[0];
                    buffer = buffer.substring(versionStr.length() + 1);
                    state = PSTATE.COLUMN;
                    nothingDone = false;
                    break;
                case COLUMN:
                    //logger.info("Inside column");
                    //logger.info(buffer);
                    if (buffer.startsWith("columnModel")) {
                        //logger.info("col model");
                        buffer = buffer.substring("columnModel".length() + 1);
                        columnModel = buffer.split("\n")[0];
                        buffer = buffer.substring(columnModel.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("maxCellIndex")) {
                        //logger.info("maxcellindex");
                        buffer = buffer.substring("maxCellIndex".length() + 1);
                        maxCellIndexStr = buffer.split("\n")[0];
                        maxCellIndex = Integer.parseInt(maxCellIndexStr);
                        buffer = buffer.substring(maxCellIndexStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("keyColumnIndex")) {
                        //logger.info("keycolindex");
                        buffer = buffer.substring("keyColumnIndex".length() + 1);
                        keyColumnIndexStr = buffer.split("\n")[0];
                        keyColumnIndex = Integer.parseInt(keyColumnIndexStr);
                        buffer = buffer.substring(keyColumnIndexStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("columnCount")) {
                        //logger.info("Inside colcount");
                        buffer = buffer.substring("columnCount".length() + 1);
                        columnCountStr = buffer.split("\n")[0];
                        columnCount = Integer.parseInt(columnCountStr);
                        columnNames = new String[columnCount];
                        buffer = buffer.substring(columnCountStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (columnCount > -1 && currentColumnsRead < columnCount){
                        //logger.info("Inside colnames");
                        String columnData = buffer.split("\n")[0];
                        buffer = buffer.substring(columnData.length() + 1);
                        JSONTokener tokener = new JSONTokener(columnData);
                        JSONObject obj;
                        try {
                            obj = (JSONObject) tokener.nextValue();
                            columnNames[currentColumnsRead++] = new String(JSONUtilities.getString(obj, "name", "Error Recovering Column name"));
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        nothingDone = false;
                    }
                    else if (columnCount > -1 && currentColumnsRead == columnCount) {
                        //logger.info("Inside to create tables");
                        createTable();
                        state = PSTATE.COLUMNGROUP;
                        nothingDone = false;
                    }
                    break;
                case COLUMNGROUP:
                    //logger.info(buffer);
                    //logger.info("Inside colgroup");
                    if (buffer.startsWith("columnGroupCount")) {
                        //logger.info("Inside colgroupCount");
                        buffer = buffer.substring("columnGroupCount".length() + 1);
                        columnGroupCountStr = buffer.split("\n")[0];
                        buffer = buffer.substring(columnGroupCountStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("/e/")){
                        //logger.info("Inside colgroupended");
                        buffer = buffer.substring("/e/".length() + 1);
                        state = PSTATE.HISTORY;
                        nothingDone = false;
                    }
                    break;
                case HISTORY:
                    ////logger.info(buffer);
                    //logger.info("Inside history");
                    if (buffer.startsWith("history")) {
                        buffer = buffer.substring("history".length() + 1);
                        historyStr = buffer.split("\n")[0];
                        buffer = buffer.substring(historyStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("pastEntryCount")) {
                        buffer = buffer.substring("pastEntryCount".length() + 1);
                        pastEntryCountStr = buffer.split("\n")[0];
                        pastEntryCount = Integer.parseInt(pastEntryCountStr);
                        buffer = buffer.substring(pastEntryCountStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("futureEntryCount")) {
                        buffer = buffer.substring("futureEntryCount".length() + 1);
                        futureEntryCountStr = buffer.split("\n")[0];
                        buffer = buffer.substring(futureEntryCountStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (pastEntryCount > -1 && currentPastEntryCount < pastEntryCount) {
                        String entryData = buffer.split("\n")[0];
                        buffer = buffer.substring(entryData.length() + 1);
                        currentPastEntryCount++;
                        nothingDone = false;
                    }
                    else if (buffer.startsWith("/e/")){
                        buffer = buffer.substring("/e/".length() + 1);
                        state = PSTATE.ROW;
                        nothingDone = false;
                    }
                    break;
                case ROW:
                    //logger.info(buffer);
                    if (buffer.startsWith("rowCount")) {
                        buffer = buffer.substring("rowCount".length() + 1);
                        rowCountStr = buffer.split("\n")[0];
                        rowCount = Integer.parseInt(rowCountStr);
                        buffer = buffer.substring(rowCountStr.length() + 1);
                        nothingDone = false;
                    }
                    else if (rowCount > -1 && currentRowsRead < rowCount) {
                        String rowData = buffer.split("\n")[0];
                        buffer = buffer.substring(((rowData.length() + 1) < buffer.length())?(rowData.length() + 1): buffer.length());
                        JSONTokener tokener = new JSONTokener(rowData);
                        JSONObject obj;
                        try {
                            obj = (JSONObject) tokener.nextValue();
                            insertRecord(obj);
                            currentRowsRead++;
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        nothingDone = false;
                    }
                    else if (rowCount > -1 && currentRowsRead == rowCount) {
                        state = PSTATE.COMPLETED;
                        nothingDone = true;
                    }
                    break;
            }
        }
        
        
    }
    @Override
    public void flush() {
        //logger.info("Flush called");
        parseAndExecute();
    }
    
    @Override
    public void write(int b)
            throws IOException {
        buffer = buffer + (char) b;
    }
    
    @Override
    public void close() throws IOException {
        flush();
        super.close();
    }

}
