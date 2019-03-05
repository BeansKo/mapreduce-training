package com.beans.hadoop.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordKeyWritable implements WritableComparable<WordKeyWritable>{

	/**
	 * 全排序时用的key
	 */
	private Text word;
	
	/**
	 * 二排排序时用的值
	 */
	private LongWritable num;
	
	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public LongWritable getNum() {
		return num;
	}

	public void setNum(LongWritable num) {
		this.num = num;
	}

	public WordKeyWritable() {
		this.word = new Text();
		this.num = new LongWritable();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.word.write(out);
		this.num.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
		this.num.readFields(in);
	}

	@Override
	public int compareTo(WordKeyWritable o) {
		return this.word.compareTo(o.getWord());
	}

	/**
	 * 此类只按word属性进行hashcode
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result =1;
//		result = prime * result + ((num == null)?0 : num.hashCode());
		result = prime * result + ((word == null)?0 : word.hashCode());
		return result;
	}

	/**
	 * 此类比较想能只能对比word属性
	 */
	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		if(obj == null)
			return false;
		if(getClass() != obj.getClass())
			return false;
		WordKeyWritable other = (WordKeyWritable)obj;
		if(word == null){
			if(other.word != null)
				return false;
		}else if (!word.equals(other.word))
			return false;
		
		return true;
	}

	@Override
	public String toString() {
		return this.word.toString();
	}
}
