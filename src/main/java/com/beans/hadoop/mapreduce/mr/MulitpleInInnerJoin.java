package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.writable.WordWritable;

/*
 * 多目录输入，并指定每个目录所用的mapper
 */
public class MulitpleInInnerJoin extends Configured implements Tool{
	private static final String SIGN1 = "\t";
	
	public static class InnerJoinMaxMapper extends Mapper<LongWritable,Text,Text,WordWritable>{
		private Text outKey = new Text();
		private WordWritable outValue = new WordWritable();
		
		/*
		 * 根据任务的不同指定数据类型
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
				outValue.setType("2");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split(SIGN1);
			if(strs.length != 2){
				return;
			}
			String word = strs[0];
			long num = Long.parseLong(strs[1]);
			outKey.set(word);
			outValue.setN(num);
			outValue.setWord(outKey);
			context.write(outKey, outValue);
		}
	}
	
	public static class InnerJoinMinMapper extends Mapper<LongWritable,Text,Text,WordWritable>{
		private Text outKey = new Text();
		private WordWritable outValue = new WordWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
				outValue.setType("1");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split(SIGN1);
			if(strs.length != 2){
				return;
			}
			String word = strs[0];
			long num = Long.parseLong(strs[1]);
			outKey.set(word);
			outValue.setN(num);
			outValue.setWord(outKey);
			context.write(outKey, outValue);
		}
	}
	
	private static class MulitpleInInnerJoinReducer extends Reducer<Text,WordWritable,Text,Text>{

		private Text outValue = new Text();
		private List<Long> firstList = new ArrayList<Long>();
		private List<Long> secondList = new ArrayList<Long>();
		@Override
		protected void reduce(Text key, Iterable<WordWritable> value,
				Context context) throws IOException, InterruptedException {
			//注意进行缓存的清理，不然下一个key的数据会被追加到每次key的后面
			firstList.clear();
			secondList.clear();
			for(WordWritable word:value){
				if(word.getType().equals("1")){
					firstList.add(word.getN());
				}else{
					secondList.add(word.getN());
				}
			}
			
			for(Long max:secondList){
				for(Long min:firstList){
					outValue.set(max+SIGN1+min);
					context.write(key, outValue);
				}
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MulitpleInInnerJoin(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"MulitpleInInnerJoin");
		job.setJarByClass(MulitpleInInnerJoin.class);
		job.setReducerClass(MulitpleInInnerJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outputDir = new Path(Constants.BASE_PATH+"/output/mulitpleininnerjoin");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		
		//设置任务的输入地址，并指定相应的mapper
		MultipleInputs.addInputPath(job, new Path(Constants.BASE_PATH+"/output/mulitipleout/maxout"), TextInputFormat.class,InnerJoinMaxMapper.class);
		MultipleInputs.addInputPath(job, new Path(Constants.BASE_PATH+"/output/mulitipleout/minout"), TextInputFormat.class,InnerJoinMinMapper.class);

		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job.waitForCompletion(true)?0:1;
	}

}
