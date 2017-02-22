/**
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


public class SpecialWritable implements Writable {

	private String url ;//专题URL　 
	private String  commentId;// 创建评论ID 
	private String channel ;// 所属频道 　 
	private String cloumn ;//所属栏目　 
	private String englishName ;//英文名　 
	private String chineseName ;//中文名
	private String pageNumber;//分页数量
	private String sourceTag;//原创标记
	private String originalAuthor ;//原创作者
	private String publishTime ;//发布时间
	private String editor ;//责编（工作代码）
	private String lmodify ;//最后一次修改时间
	


    public SpecialWritable() {    }

    public SpecialWritable(SpecialWritable one) {
    	
    	

    	this.url =one.url; 
    	this.commentId =one.commentId; 
       	this.channel =one.channel; 
    	this.cloumn  = one.cloumn;
    	this.englishName  = one.englishName;
    	this.chineseName =one.chineseName; 
    	this.pageNumber =one.pageNumber; 
     	this.sourceTag =one.sourceTag; 
     	this.originalAuthor =one.originalAuthor;  	
    	this.publishTime =one.publishTime;
       	this.editor =one.editor; 
      	this.lmodify =one.lmodify; 
      	
      	
    }

    /**
     * @param first
     * @param second
     */
    public SpecialWritable(
        	
        String url,
        String  commentId,
        String channel , 
        String cloumn, 
        String englishName,
        String chineseName ,
        String pageNumber,
        String sourceTag,
        String originalAuthor,
        String publishTime ,
        String editor ,
        String lmodify ) {

    	this.url =url; 
    	this.commentId =commentId; 
       	this.channel =channel; 
    	this.cloumn  = cloumn;
    	this.englishName  = englishName;
    	this.chineseName =chineseName; 
    	this.pageNumber =pageNumber; 
     	this.sourceTag =sourceTag; 
     	this.originalAuthor =originalAuthor;  	
    	this.publishTime =publishTime;
       	this.editor =editor; 
      	this.lmodify =lmodify; 
    }





	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getCommentId() {
		return commentId;
	}

	public void setCommentId(String commentId) {
		this.commentId = commentId;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getCloumn() {
		return cloumn;
	}

	public void setCloumn(String cloumn) {
		this.cloumn = cloumn;
	}

	public String getEnglishName() {
		return englishName;
	}

	public void setEnglishName(String englishName) {
		this.englishName = englishName;
	}

	public String getChineseName() {
		return chineseName;
	}

	public void setChineseName(String chineseName) {
		this.chineseName = chineseName;
	}

	public String getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(String pageNumber) {
		this.pageNumber = pageNumber;
	}

	public String getSourceTag() {
		return sourceTag;
	}

	public void setSourceTag(String sourceTag) {
		this.sourceTag = sourceTag;
	}

	public String getOriginalAuthor() {
		return originalAuthor;
	}

	public void setOriginalAuthor(String originalAuthor) {
		this.originalAuthor = originalAuthor;
	}

	public String getPublishTime() {
		return publishTime;
	}

	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
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

	     url = in.readUTF() ; 
		 commentId = in.readUTF(); 
		 channel = in.readUTF(); 
		 cloumn = in.readUTF();
		 englishName = in.readUTF();
		 chineseName = in.readUTF(); 
		 pageNumber = in.readUTF(); 
		 sourceTag = in.readUTF(); 
		 originalAuthor = in.readUTF();  	
		 publishTime = in.readUTF();
		 editor = in.readUTF(); 
		 lmodify = in.readUTF();     	
		
      }

    public void write(DataOutput out) throws IOException {


    	HadoopUtils.writeString(out, url);
       	HadoopUtils.writeString(out, commentId);
    	HadoopUtils.writeString(out, channel);
    	HadoopUtils.writeString(out, cloumn);
    	HadoopUtils.writeString(out, englishName);
    	HadoopUtils.writeString(out, chineseName);
    	HadoopUtils.writeString(out, pageNumber);
    	HadoopUtils.writeString(out, sourceTag);
    	HadoopUtils.writeString(out, originalAuthor);
    	HadoopUtils.writeString(out, publishTime);
    	HadoopUtils.writeString(out, editor);
    	HadoopUtils.writeString(out, lmodify);

    
    }

  
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(url).append("\t").append(commentId).append("\t").append(channel).append("\t").append(cloumn)
    	.append("\t").append(englishName).append("\t").append(chineseName).append("\t").append(pageNumber)
    	.append("\t").append(sourceTag).append("\t").append(originalAuthor).append("\t").append(publishTime)
    	.append("\t").append(editor).append("\t").append(lmodify);
        return  sb.toString();
    }
}
