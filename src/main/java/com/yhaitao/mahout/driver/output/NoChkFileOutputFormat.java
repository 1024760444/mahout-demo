package com.yhaitao.mahout.driver.output;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

/**
 * 对输出路径不做校验。 默认，输出路径存在，则异常。
 * @author yhaitao
 */
public class NoChkFileOutputFormat extends SequenceFileOutputFormat<Text, VectorWritable> {
	/**
	 * 不对文件输出路径做任何校验
	 */
	public void checkOutputSpecs(JobContext job) 
			throws FileAlreadyExistsException, IOException {}
}
