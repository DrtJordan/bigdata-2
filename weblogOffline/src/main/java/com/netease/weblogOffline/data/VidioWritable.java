/**
 * @(#)LongLongWritable.java, 2012-11-20. 
 * 
 * Copyright 2012 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.netease.weblogOffline.utils.HadoopUtils;


public class VidioWritable implements Writable {



	private String vId ;//视频id
	private String url;//视频url
	private String title ;//标题 　 
	private String tag ;//标签 
	private String originalAuthor ;//ORIGINALAUTHOR 原创作者 　
	private String upPerson ;//上传者
	private String modifyPerson ;//修改者　 
	private String source ;//SOURCE 来源 　 
	private String gentieId ;//跟帖id　 
	private String upTime ;//上传时间
	private String editor ;//责编
	private String lmodify ;//lmodify

    public VidioWritable() {    }

    public VidioWritable(VidioWritable one) {
 
    	
    	this.vId = one.vId ;
    	this.url = one.url;
    	this.title = one.title; 
    	this.tag =one.tag;
    	this.originalAuthor =one.originalAuthor;
    	this.upPerson = one.upPerson; 
    	this.modifyPerson =one.modifyPerson; 
    	this.source =one.source; 
    	this.gentieId =one.gentieId;
    	this.upTime =one.upTime;
    	this.editor =one.editor; 
    	this.lmodify=one.lmodify;
    }

    /**
     * @param first
     * @param second
     */
    public VidioWritable(String vId,
	String url,
	String title,
	String tag,
	String originalAuthor,
	String upPerson,
	String modifyPerson,
	String source,
	String gentieId, 
	String upTime,
	String editor,
	String lmodify) {
     	this.vId = vId ;
    	this.url = url;
    	this.title = title; 
    	this.tag =tag;
    	this.originalAuthor =originalAuthor;
    	this.upPerson = upPerson; 
    	this.modifyPerson =modifyPerson; 
    	this.source =source; 
    	this.gentieId =gentieId;
    	this.upTime =upTime;
    	this.editor =editor; 
    	this.lmodify=lmodify;
    }


	public String getvId() {
		return vId;
	}

	public void setvId(String vId) {
		this.vId = vId;
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

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getOriginalAuthor() {
		return originalAuthor;
	}

	public void setOriginalAuthor(String originalAuthor) {
		this.originalAuthor = originalAuthor;
	}

	public String getUpPerson() {
		return upPerson;
	}

	public void setUpPerson(String upPerson) {
		this.upPerson = upPerson;
	}

	public String getModifyPerson() {
		return modifyPerson;
	}

	public void setModifyPerson(String modifyPerson) {
		this.modifyPerson = modifyPerson;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getGentieId() {
		return gentieId;
	}

	public void setGentieId(String gentieId) {
		this.gentieId = gentieId;
	}

	public String getUpTime() {
		return upTime;
	}

	public void setUpTime(String upTime) {
		this.upTime = upTime;
	}

	public String getEditor() {
		return editor;
	}

	public void setEditor(String editor) {
		this.editor = editor;
	}

	public String getLmodify() {
		return lmodify;
	}

	public void setLmodify(String lmodify) {
		this.lmodify = lmodify;
	}

	public void readFields(DataInput in) throws IOException {
    	
    	vId = in.readUTF();
    	url = in.readUTF();
    	title = in.readUTF();
    	tag = in.readUTF();
    	originalAuthor= in.readUTF();
    	upPerson= in.readUTF();
    	modifyPerson= in.readUTF();
		source= in.readUTF();
		gentieId= in.readUTF();
		upTime= in.readUTF();
		editor= in.readUTF();
		lmodify= in.readUTF();
		
      }

    public void write(DataOutput out) throws IOException {

    	HadoopUtils.writeString(out, vId);
       	HadoopUtils.writeString(out, url);
    	HadoopUtils.writeString(out, title);
    	HadoopUtils.writeString(out, tag);
    	HadoopUtils.writeString(out, originalAuthor);
    	HadoopUtils.writeString(out, upPerson);
    	HadoopUtils.writeString(out, modifyPerson);
    	HadoopUtils.writeString(out, source);
    	HadoopUtils.writeString(out, gentieId);
    	HadoopUtils.writeString(out, upTime);
    	HadoopUtils.writeString(out, editor);
    	HadoopUtils.writeString(out, lmodify);
    
    }

  
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(vId).append("\t").append(url).append("\t").append(title).append("\t").append(tag)
    	.append("\t").append(originalAuthor).append("\t").append(upPerson).append("\t").append(modifyPerson)
    	.append("\t").append(source).append("\t").append(gentieId).append("\t").append(upTime)
    	.append("\t").append(editor).append("\t").append(lmodify);
        return  sb.toString();
    }
}
