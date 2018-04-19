package com.yhaitao.mahout.lookalike;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

public class LookalikeDriver extends AbstractJob {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new LookalikeDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
