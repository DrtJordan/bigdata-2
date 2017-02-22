package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class YueGaoInfo  implements Writable {
	private int pv = 0;

	private int uv = 0;

	private int genTieCount = 0;

	private int genTieUv = 0;

	private int shareCount = 0;

	private int backCount = 0;
	private   String urlinfo="";
	public YueGaoInfo(){
		
	}
	public YueGaoInfo(int pv, int uv, int genTieCount, int genTieUv,
			int shareCount, int backCount, String urlinfo) {
		super();
		this.pv = pv;
		this.uv = uv;
		this.genTieCount = genTieCount;
		this.genTieUv = genTieUv;
		this.shareCount = shareCount;
		this.backCount = backCount;
		this.urlinfo = urlinfo;
	}

	public int getPv() {
		return pv;
	}

	public void setPv(int pv) {
		this.pv = pv;
	}

	public int getUv() {
		return uv;
	}

	public void setUv(int uv) {
		this.uv = uv;
	}

	public int getGenTieCount() {
		return genTieCount;
	}

	public void setGenTieCount(int genTieCount) {
		this.genTieCount = genTieCount;
	}

	public int getGenTieUv() {
		return genTieUv;
	}

	public void setGenTieUv(int genTieUv) {
		this.genTieUv = genTieUv;
	}

	public int getShareCount() {
		return shareCount;
	}

	public void setShareCount(int shareCount) {
		this.shareCount = shareCount;
	}

	public int getBackCount() {
		return backCount;
	}

	public void setBackCount(int backCount) {
		this.backCount = backCount;
	}

	public String getUrlinfo() {
		return urlinfo;
	}

	public void setUrlinfo(String urlinfo) {
		this.urlinfo = urlinfo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.pv);
		out.writeInt(this.uv);
		out.writeInt(this.genTieCount);
		out.writeInt(this.genTieUv);
		out.writeInt(this.shareCount);
		out.writeInt(this.backCount);
		out.writeUTF(urlinfo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
	
		this.pv = in.readInt();
		this.uv = in.readInt();
		this.genTieCount = in.readInt();
		this.genTieUv = in.readInt();
		this.shareCount = in.readInt();
		this.backCount = in.readInt();
		this.urlinfo=in.readUTF();
	}

}
