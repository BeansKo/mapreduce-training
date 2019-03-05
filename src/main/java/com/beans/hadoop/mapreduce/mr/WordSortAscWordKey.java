package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.writable.WordKeyWritable;

/**
 * 二次排序
 * @author Frank
 *
 */
public class WordSortAscWordKey extends Configured implements Tool{

	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordSortAscWordKey(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"wordsortascwordkey");
		job.setJarByClass(WordSortAscWordKey.class);
		job.setMapperClass(WordSortAscWordKeyMapper.class);
		job.setReducerClass(WordSortAscWordKeyReducer.class);
		//设置排序方法可进行二次排序比较
		job.setSortComparatorClass(WordSortAscWordKeyComparator.class);
		job.setPartitionerClass(WordSortAscWordKeyPartition.class);
		//设置按什么进行分组
		job.setGroupingComparatorClass(WordKeyGroupComparator.class);
		
		job.setMapOutputKeyClass(WordKeyWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(WordKeyWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		Path inputDir = new Path(Constants.BASE_PATH+"/input/WordSortAscKeyWord.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/WordSortAscKeyWord");
		FileInputFormat.addInputPath(job, inputDir);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job.waitForCompletion(true)?0:1;
	}

	private static class WordSortAscWordKeyMapper extends Mapper<LongWritable, Text, WordKeyWritable, LongWritable>{

		private WordKeyWritable outKey = new WordKeyWritable();
		private LongWritable outValue = new LongWritable();
		private Text outWord = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split("\t");
			if(strs.length != 2){
				return;
			}
			long num = Long.parseLong(strs[1]);
			String word = strs[0];
			outValue.set(num);
			outWord.set(word);
			outKey.setWord(outWord);
			outKey.setNum(outValue);
			
			context.write(outKey, outValue);
 		}
	}
	
	private static class  WordSortAscWordKeyReducer extends org.apache.hadoop.mapreduce.Reducer<WordKeyWritable, LongWritable, WordKeyWritable, LongWritable>{

		@Override
		protected void reduce(WordKeyWritable key, Iterable<LongWritable> value,
				Context context)
				throws IOException, InterruptedException {
			for(LongWritable v:value){
				//使用了key值得toString方法
				context.write(key, v);
			}
		}
	}
	
	/**
	 * 按WordKeyWritable类的word属性的hashcode进行partition计算
	 * @author Frank
	 *
	 */
	private static class WordSortAscWordKeyPartition extends Partitioner<WordKeyWritable, LongWritable>{

		@Override
		public int getPartition(WordKeyWritable key, LongWritable value, int numPartitions) {
			return (key.getWord().hashCode() & Integer.MAX_VALUE)%numPartitions;
		}
		
	}
	
	/**
	 * 二次排序的规则，首先对比WrodKeyWritable中的Word
	 * 如果Word相等，则对比num
	 * @author Frank
	 *
	 */
	private static class WordSortAscWordKeyComparator extends WritableComparator{
		public WordSortAscWordKeyComparator(){
			super(WordKeyWritable.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			WordKeyWritable key1 = (WordKeyWritable) a;
			WordKeyWritable key2 = (WordKeyWritable) b;
			int compareTo = key1.getWord().compareTo(key2.getWord());
			if(compareTo != 0){
				return -compareTo;
			}else {
				return key1.getNum().compareTo(key2.getNum());
			}
		}
		
	}
	
	/**
	 * 进入reduce的数据的分组规则
	 * 这里只按WordKeyWritable的Word属性为分组依据
	 */
	private static class WordKeyGroupComparator extends WritableComparator{

		public WordKeyGroupComparator() {
			super(WordKeyWritable.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			WordKeyWritable key1 = (WordKeyWritable) a;
			WordKeyWritable key2 = (WordKeyWritable) b;
			return key1.getWord().compareTo(key2.getWord());
		}
		
	}
}
