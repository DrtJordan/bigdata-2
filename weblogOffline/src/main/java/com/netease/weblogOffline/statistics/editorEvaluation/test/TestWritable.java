package com.netease.weblogOffline.statistics.editorEvaluation.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.netease.weblogOffline.utils.HadoopUtils;

public class TestWritable implements Writable {
	
	private String s;
	
	public TestWritable(String s) {
		this.s = s;
	}
	
	public static void main(String[] args) {
		try {
			byte[] b = (new TestWritable("hello")).getBytes();
			System.out.println(b);
			
			TestWritable tw = new TestWritable("");
			System.out.println(tw);
			tw.readFromBytes(b);
			System.out.println(tw);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public byte[] getBytes() throws IOException{
		ByteArrayOutputStream s = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(s);
		this.write(dos);
		return s.toByteArray();
	}
	
	@Override
	public String toString() {
		return "s=" + s;
	}
	
	public void readFromBytes(byte[] bytes) throws IOException{
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		readFields(dis);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.s = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		HadoopUtils.writeString(out, s);		
	}

}
