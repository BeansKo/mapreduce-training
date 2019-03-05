package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

/**
 * word count 倒序输出
 */
public class WordSortDescPartitioner extends Configured implements Tool{

	private static class WordSortDescMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

		private Text outValue = new Text();
		private LongWritable outKey = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split("\t");
			if(strs.length != 2){
				return;
			}
			
			Long num = Long.parseLong(strs[1]);
			outKey.set(num);
			outValue.set(str);
			context.write(outKey, outValue);
		}
	}
	
	private static class WordSortDescReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			for(Text v:value){
				context.write(NullWritable.get(), v);
			}
		}
		
	}
	
	private static class LongKeyDescComparator extends WritableComparator{

		@SuppressWarnings("unused")
		public LongKeyDescComparator() {
			super(LongWritable.class,true);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
	private static class LongKeyPartitioner extends Partitioner<LongWritable, Text>{

		private static final long MAX = 100L;
		
		/**
		 * 小于100的数据均等分partioner
		 * @param key     map输出的key值
		 * @param value   map输出的value值
		 * @param numReduceTasks 用户自己设置的reducer数量
		 * @return        计算出的partition的ID
		 */
		@Override
		public int getPartition(LongWritable key, Text value, int numReduceTasks) {
			//如果reducer小于1的时候就不用计算了，所有key对应partition ID都为0
			if(numReduceTasks <=1){
				return 0;
			}
			//如果key大于最大值100，那就直接到最后一个partition中，如果放在前面的partition中，那么排序就乱了。
			long n = key.get();
			if(n>=MAX){
				int partitionerId = numReduceTasks -1;
				System.out.println("key:"+n+",partitioner:"+partitionerId);
				return partitionerId;
			}
			//根据reduce的数量，先算出数据的临界点
			int s = (int)((MAX/numReduceTasks)+1);
			//根据key的值算出自己对应的partition区间
			int partitionerId = (int)(n/s);
			System.out.println("key:"+n+",partitioner:"+partitionerId);
			return partitionerId;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"wordsortasc");
		job.setJarByClass(WordSortDescPartitioner.class);
		job.setMapperClass(WordSortDescMapper.class);
		job.setReducerClass(WordSortDescReducer.class);
		job.setSortComparatorClass(LongKeyDescComparator.class);
		//设置计算partitioner
		job.setPartitionerClass(LongKeyPartitioner.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(2);
		Path inputDir = new Path(Constants.BASE_PATH+"/input/WordSortDescPartitioner.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/WordSortDescPartitioner");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordSortDescPartitioner(), args));
	}
}
