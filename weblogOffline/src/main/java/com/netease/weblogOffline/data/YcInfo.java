package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.netease.weblogOffline.utils.HadoopUtils;

public class YcInfo implements Writable {
	private String lmodify;//最后修改时间
	private String url;
	private String title;
	private String ptime;//发布时间
	private String author;
	
	public YcInfo() {}
	
	public YcInfo(YcInfo ycInfo) {
		lmodify = ycInfo.lmodify;
		url = ycInfo.url;
		title = ycInfo.title;
		ptime =  ycInfo.ptime;
		author = ycInfo.author;
	}
	
	@Override
	public String toString() {
		return lmodify + "\t" + url + "\t" + title + "\t" + ptime + "\t" + author;
	}

	public String getLmodify() {
		return lmodify;
	}

	public void setLmodify(String lmodify) {
		this.lmodify = lmodify;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPtime() {
		return ptime;
	}

	public void setPtime(String ptime) {
		this.ptime = ptime;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lmodify = in.readUTF();
		this.url = in.readUTF();
		this.title = in.readUTF();
		this.ptime = in.readUTF();
		this.author = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		HadoopUtils.writeString(out, lmodify);
		HadoopUtils.writeString(out, url);
		HadoopUtils.writeString(out, title);
		HadoopUtils.writeString(out, ptime);
		HadoopUtils.writeString(out, author);
	}

}
