package com.beans.hadoop.mapreduce.mrmutil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.beans.hadoop.mapreduce.base.BaseMR;
import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.writable.WordWritable;

public class InnerJoin extends BaseMR{
	private static final String SIGN1 = "\t";
//	private static final String SIGN2 = "\001";
	
	public static class InnerJoinMapper extends Mapper<LongWritable,Text,Text,WordWritable>{
		private Text outKey = new Text();
		private WordWritable outValue = new WordWritable();
		
		/*
		 * 可以从这个map任务的输入文件的目录名称来判断是属于哪类数据从而进行数据的分类
		 * map任务中使用context对象可以获得本次任务的输入文件地址
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//获取输入文件
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			//获取输入文件路径
			String path = inputSplit.getPath().toString();
			if(path.contains("min")){
				outValue.setType("1");
			}else if(path.contains("max")){
				outValue.setType("2");
			}
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
	
	private static class InnerJoinReducer extends Reducer<Text,WordWritable,Text,Text>{

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

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf,"InnerJoin");
		job.setJarByClass(InnerJoin.class);
		job.setMapperClass(InnerJoinMapper.class);
		job.setReducerClass(InnerJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outputDir = new Path(Constants.BASE_PATH+"/output/innerjoin");
		//设置多个目录为输入目录，用逗号分隔
		FileInputFormat.setInputPaths(job, Constants.BASE_PATH+"/input/mulitiple/max,"+Constants.BASE_PATH+"/input/mulitiple/min");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job;
	}

	@Override
	public String getJobName() {
		return "InnerJoin";
	}

}
