package com.beans.hadoop.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordWritable implements Writable{

	public Long getN() {
		return n;
	}

	public void setN(Long n) {
		this.n = n;
	}

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	private Long n;
	private Text word = new Text();
	private String type="";
	
	//字段的读写顺序要一致
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(n);
		//序列化自己的方法
		word.write(out);
		out.writeUTF(type);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		n = in.readLong();
		word.readFields(in);
		type = in.readUTF();
	}

}
