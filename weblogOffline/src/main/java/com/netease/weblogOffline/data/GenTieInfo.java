package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GenTieInfo  implements Writable {
	private String pdocid = "";

	private String docid = "";

	private String channel = "";

	private  int  count=0;
	public GenTieInfo(){
		
	}


	public GenTieInfo(String pdocid, String docid, String channel, int count) {
		super();
		this.pdocid = pdocid;
		this.docid = docid;
		this.channel = channel;
		this.count = count;
	}




	public String getPdocid() {
		return pdocid;
	}


	public void setPdocid(String pdocid) {
		this.pdocid = pdocid;
	}


	public String getDocid() {
		return docid;
	}


	public void setDocid(String docid) {
		this.docid = docid;
	}


	public String getChannel() {
		return channel;
	}


	public void setChannel(String channel) {
		this.channel = channel;
	}


	public int getCount() {
		return count;
	}


	public void setCount(int count) {
		this.count = count;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	  out.writeUTF(this.pdocid);
	  out.writeUTF(this.docid);
	  out.writeUTF(this.channel);
	  out.writeInt(this.count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
	
		this.pdocid = in.readUTF();
		this.docid = in.readUTF();
		this.channel = in.readUTF();
		this.count=in.readInt();
	}

}
