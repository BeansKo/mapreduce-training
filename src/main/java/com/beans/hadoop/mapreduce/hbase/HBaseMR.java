package com.beans.hadoop.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseMR {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);

		job.setJobName("HBase MR");
		job.setJarByClass(HBaseMR.class);

		// 设置输入格式
		job.setInputFormatClass(TableInputFormat.class);

		// 配置输入表
		TableInputFormat.configureSplitTable(job, TableName.valueOf("ecitem:CallLogs"));
		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "ecitem:CallLogs");

		byte[][] bytes = { Bytes.toBytes("f1:MyCallNo"), Bytes.toBytes("f1:OtherCallNo"),
				Bytes.toBytes("f1:CallTime") };

		TableInputFormat.addColumns(new Scan(), bytes);

		FileOutputFormat.setOutputPath(job, new Path("d:/mr/out"));

		job.setMapperClass(MapCount.class);
		job.setReducerClass(ReduceCount.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}

	/**
	 * TableMapper,对hbase表中的记录进行变换
	 */
	public static class MapCount extends TableMapper<Text, IntWritable> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)
				throws IOException, InterruptedException {
			String myCallNo = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("MyCallNo")));
			String otherCallNo = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("OtherCallNo")));
			String callTime = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("CallTime")));
			callTime = callTime.substring(0, 6);
			
			Text key1 = new Text(myCallNo + "," + callTime);
			Text key2 = new Text(otherCallNo + "," + callTime);
			IntWritable outValue = new IntWritable(1);
			
			context.write(key1, outValue);
			context.write(key2, outValue);
		}
	}
	
	/**
	 * 统计Reduce
	 */
	public static class ReduceCount extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : values){
				count = count + i.get();
			}
			
			context.write(key, new IntWritable(count/2));
		}
		
	}

}
