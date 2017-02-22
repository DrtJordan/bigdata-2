package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * 内容打分向量,用于记录各维度当天新增数据 新增项时，更新keySize大小，必须按顺序追加到末尾
 * */
public class ContentScoreVector implements Writable {
	// 所有有效数据项个数，不包括该项本身
	private int keySize = 13;

	private String url = "";

	private String channel = "";

	private String source = "";

	private String type = "";

	private String lmodify = "";

	private String title = "";
	
	private String author = "";

	private int pv = 0;

	private int uv = 0;

	private int genTieCount = 0;

	private int genTieUv = 0;

	private int shareCount = 0;

	private int backCount = 0;

	public ContentScoreVector() {}

	public ContentScoreVector(ContentScoreVector csw) {
		this.url = csw.url;
		this.channel = csw.channel;
		this.source = csw.source;
		this.type = csw.type;
		this.lmodify = csw.lmodify;
		this.title = csw.title;
		this.author = csw.author;
		this.pv = csw.pv;
		this.uv = csw.uv;
		this.genTieCount = csw.genTieCount;
		this.genTieUv = csw.genTieUv;
		this.shareCount = csw.shareCount;
		this.backCount = csw.backCount;
	}

	public boolean isYc() {
		return "yc".equals(source);
	}

	@Override
	public String toString() {
		return "url=" + url + ",channel=" + channel + ",source=" + source
				+ ",type=" + type + ",lmodify=" + lmodify + ",title=" + title
				+ ",author=" + author + ",pv=" + pv + ",uv=" + uv
				+ ",genTieCount=" + genTieCount + ",genTieUv=" + genTieUv
				+ ",shareCount=" + shareCount + ",backCount=" + backCount;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.keySize);
		HadoopUtils.writeString(out, this.url);
		HadoopUtils.writeString(out, this.channel);
		HadoopUtils.writeString(out, this.source);
		HadoopUtils.writeString(out, this.type);
		HadoopUtils.writeString(out, this.lmodify);
		HadoopUtils.writeString(out, this.title);
		HadoopUtils.writeString(out, this.author);
		out.writeInt(this.pv);
		out.writeInt(this.uv);
		out.writeInt(this.genTieCount);
		out.writeInt(this.genTieUv);
		out.writeInt(this.shareCount);
		out.writeInt(this.backCount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.keySize = in.readInt();
		int count = this.keySize;
		if (count-- > 0) {
			this.url = in.readUTF();
		}
		if (count-- > 0) {
			this.channel = in.readUTF();
		}
		if (count-- > 0) {
			this.source = in.readUTF();
		}
		if (count-- > 0) {
			this.type = in.readUTF();
		}
		if (count-- > 0) {
			this.lmodify = in.readUTF();
		}
		if (count-- > 0) {
			this.title = in.readUTF();
		}
		if (count-- > 0) {
			this.author = in.readUTF();
		}
		if (count-- > 0) {
			this.pv = in.readInt();
		}
		if (count-- > 0) {
			this.uv = in.readInt();
		}
		if (count-- > 0) {
			this.genTieCount = in.readInt();
		}
		if (count-- > 0) {
			this.genTieUv = in.readInt();
		}
		if (count-- > 0) {
			this.shareCount = in.readInt();
		}
		if (count-- > 0) {
			this.backCount = in.readInt();
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getLmodify() {
		return lmodify;
	}

	public void setLmodify(String lmodify) {
		this.lmodify = lmodify;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
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

	public int getKeySize() {
		return keySize;
	}
}
