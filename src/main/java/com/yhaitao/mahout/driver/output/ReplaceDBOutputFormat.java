package com.yhaitao.mahout.driver.output;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 输出数据到Mysql；如果主键存在，则替换。
 * @author yhaitao
 *
 */
public class ReplaceDBOutputFormat extends DBOutputFormat<DBWritable, NullWritable> {
  /**
   * Initializes the reduce-part of the job with 
   * the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldNames The field names in the table.
   */
  public static void setOutput(Job job, String tableName, 
      String... fieldNames) throws IOException {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if (fieldNames.length > 0) {
        setOutput(job, tableName, fieldNames.length);
      }
      else { 
        throw new IllegalArgumentException(
          "Field names must be greater than 0");
      }
    }
  }
  
  private static DBConfiguration setOutput(Job job,
        String tableName) throws IOException {
      job.setOutputFormatClass(ReplaceDBOutputFormat.class);
      job.setReduceSpeculativeExecution(false);

      DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
      
      dbConf.setOutputTableName(tableName);
      return dbConf;
    }
  
  /**
   * Constructs the query used as the prepared statement to insert data.
   * 
   * INSERT INTO t_cq_day_citycode_flow_wlanPower (pdate,citycode,subDeviceNumber,wlanPowerAvg) 
   * VALUES ('2018-01-02','023','45','2.5') 
   * ON DUPLICATE KEY UPDATE subDeviceNumber=VALUES(subDeviceNumber),wlanPowerAvg=VALUES(wlanPowerAvg);
   * 
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  public String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("REPLACE INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");

    return query.toString();
  }
}
