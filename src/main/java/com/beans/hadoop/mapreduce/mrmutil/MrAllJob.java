package com.beans.hadoop.mapreduce.mrmutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class MrAllJob extends Configured implements Tool{

	public static void main(String[] args) {

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		return 0;
	}

}
