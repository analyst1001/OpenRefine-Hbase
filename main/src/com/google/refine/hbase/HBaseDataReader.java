package com.google.refine.hbase;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseDataReader {
    private static Configuration conf = null;
    Project project;
    final static Logger logger = LoggerFactory.getLogger("project_utilities");
    public  HBaseDataReader(Project proj) {
        project = proj;
        conf = (new HBaseConfiguration()).create();
    }
    
    public void loadFromTable() throws IOException {
        HTable table = new HTable(conf, String.valueOf(project.id));
        int index = 0;
        Collection<HColumnDescriptor> descs = table.getTableDescriptor().getFamilies();
        project.columnModel._maxCellIndex = descs.size() - 2;
        project.columnModel._keyColumnIndex = 0;
        for (HColumnDescriptor desc : descs) {
            String colName = desc.getNameAsString();
            if ((colName.compareTo("starred") != 0) && (colName.compareTo("flagged") != 0))  {
                Column column = new Column(index, colName);
                column._name = colName;
                try {
                    project.columnModel.addColumn(index,column, true);
                } catch (ModelException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                index++;
            }
        }
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Row row = new Row(index);
            row.starred =  Boolean.parseBoolean(rr.getValue(Bytes.toBytes("starred"), Bytes.toBytes("starred")).toString());
            row.flagged =  Boolean.parseBoolean(rr.getValue(Bytes.toBytes("flagged"), Bytes.toBytes("flagged")).toString());
            
            for (Column col : project.columnModel.columns) {
                Cell cell = new Cell(new String(rr.getValue(Bytes.toBytes(col.getName()), Bytes.toBytes(col.getName()))), null);
                //logger.info("Reading column " + col.getName() + " has value "+ new String(rr.getValue(Bytes.toBytes(col.getName()), Bytes.toBytes(col.getName()))));
                row.cells.add(cell);
            }
            project.rows.add(row);
        }
        
    }
}
