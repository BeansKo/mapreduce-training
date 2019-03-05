package com.beans.hadoop.mapreduce.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

public class SemiJoin extends Configured implements Tool{
	private static final String SIGN1 = "\t";
	
	private static class SemiJoinMapper extends Mapper<LongWritable,Text,Text,Text>{

		private Map<String,String> minCacheMap = new HashMap<String,String>();
		private Text outKey = new Text();
		private Text outValue = new Text();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			if(cacheFiles.length != 1){
				return;
			}
			
			String path = cacheFiles[0].toString();
			System.out.println(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("min.dat"), "UTF-8"));
			String data;
			while((data = br.readLine()) != null){
				String tmp[] = data.split(SIGN1);
				String word = tmp[0];
				String num = tmp[1];
				minCacheMap.put(word, num);
			}
			System.out.println("cache size:"+minCacheMap.size());
			br.close();
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
			Long maxNum = Long.parseLong(strs[1]);
			String minNum = minCacheMap.get(word);
			outKey.set(word);
			outValue.set(maxNum + SIGN1 + minNum);
			context.write(outKey, outValue);
		}


		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SemiJoin(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"semijoin");
		File file = new File(Constants.BASE_PATH+"/input/min.dat");
		//增加缓存文件
		job.addCacheFile(file.toURI());
		job.setJarByClass(SemiJoin.class);
		job.setMapperClass(SemiJoinMapper.class);
		//表示这个任务不需要reduce
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputDir = new Path(Constants.BASE_PATH+"/output/mulitipleout/maxout");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/semijoin");
		FileInputFormat.addInputPath(job, inputDir);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}

}
