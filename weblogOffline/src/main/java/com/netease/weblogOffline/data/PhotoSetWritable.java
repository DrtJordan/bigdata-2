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


public class PhotoSetWritable implements Writable {



	private String setId ;//图集id 图片库数据库已有 　 
	private String setName ;// 图集标题 　 
	private String topicId ;//所属栏目 　 
	private String createTime ;// 创建时间 　 
	private String imgSum ;//IMGSUM 图片个数 　 
	private String cover ;//COVER 封面id 　 
	private String userId ;//USERID 作者（比如bjsunying） 　 
	private String postId ;//POSTID 跟帖id 　 
	private String source ;//SOURCE 来源 　 
	private String reporter ;//REPORTER 记者 　 
	private String nickName ;//NICKNAME 作者（比如bjsunying） 　 
	private String keyWord ;//KEYWORD 关键词 　 
	private String firstPublish ;//FIRSTPUBLISH 第一次发布时间？ 需要图片库（王龙）确认 
	private String clientSetName ;//CLIENTSETNAME 客户端用的短标题 　 
	private String originalAuthor ;//ORIGINALAUTHOR 原创作者 　 
	private String originalType ;//ORIGINALTYPE 原创类型 　 
	private String setUrl ;//图集URL 需要添加 　 
	private String channel ;//图集URL 需要添加 　 
	private String editor ;//图集URL 需要添加 　 
	private String lmodify ;//lmodify

    public PhotoSetWritable() {    }

    public PhotoSetWritable(PhotoSetWritable one) {

    	this.setId = one.setId ;
    	this.setName = one.setName;
    	this.topicId = one.topicId; 
    	this.createTime =one.createTime;
    	this.imgSum =one.imgSum;
    	this.cover = one.cover; 
    	this.userId =one.userId; 
    	this.postId =one.postId; 
    	this.source =one.source; 
    	this.reporter =one.reporter;
    	this.nickName =one.nickName;
    	this.keyWord =one.keyWord; 
    	this.firstPublish =one.firstPublish; 
    	this.clientSetName =one.clientSetName; 
    	this.originalAuthor =one.originalAuthor; 
    	this.originalType =one.originalType;
    	this.setUrl =one.setUrl ;
    	this.channel  = one.channel;
        this.editor = one.editor ;
    	this.lmodify=one.lmodify;
    }

    /**
     * @param first
     * @param second
     */
    public PhotoSetWritable(String setId,
	String setName,
	String topicId,
	String createTime,
	String imgSum,
	String cover,
	String userId,
	String postId,
	String source, 
	String reporter,
	String nickName,
	String keyWord, 
	String firstPublish,
	String clientSetName, 
	String originalAuthor,
	String originalType,
	String setUrl,
	String channel,
    String editor ,
	String lmodify) {
        super();
    	this.setId = setId ;
    	this.setName = setName;
    	this.topicId = topicId; 
    	this.createTime =createTime;
    	this.imgSum =imgSum;
    	this.cover = cover; 
    	this.userId =userId; 
    	this.postId =postId; 
    	this.source =source; 
    	this.reporter =reporter;
    	this.nickName =nickName;
    	this.keyWord =keyWord; 
    	this.firstPublish =firstPublish; 
    	this.clientSetName =clientSetName; 
    	this.originalAuthor =originalAuthor; 
    	this.originalType =originalType;
    	this.setUrl =setUrl ;
    	this.channel  = channel;
        this.editor = editor ;
    	this.lmodify=lmodify;
    }



	

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getEditor() {
		return editor;
	}

	public void setEditor(String editor) {
		this.editor = editor;
	}

	public String getSetId() {
		return setId;
	}

	public void setSetId(String setId) {
		this.setId = setId;
	}

	public String getSetName() {
		return setName;
	}

	public void setSetName(String setName) {
		this.setName = setName;
	}

	public String getTopicId() {
		return topicId;
	}

	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public String getImgSum() {
		return imgSum;
	}

	public void setImgSum(String imgSum) {
		this.imgSum = imgSum;
	}

	public String getCover() {
		return cover;
	}

	public void setCover(String cover) {
		this.cover = cover;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPostId() {
		return postId;
	}

	public void setPostId(String postId) {
		this.postId = postId;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getReporter() {
		return reporter;
	}

	public void setReporter(String reporter) {
		this.reporter = reporter;
	}

	public String getNickName() {
		return nickName;
	}

	public void setNickName(String nickName) {
		this.nickName = nickName;
	}

	public String getKeyWord() {
		return keyWord;
	}

	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}

	public String getFirstPublish() {
		return firstPublish;
	}

	public void setFirstPublish(String firstPublish) {
		this.firstPublish = firstPublish;
	}

	public String getClientSetName() {
		return clientSetName;
	}

	public void setClientSetName(String clientSetName) {
		this.clientSetName = clientSetName;
	}

	public String getOriginalAuthor() {
		return originalAuthor;
	}

	public void setOriginalAuthor(String originalAuthor) {
		this.originalAuthor = originalAuthor;
	}

	public String getOriginalType() {
		return originalType;
	}

	public void setOriginalType(String originalType) {
		this.originalType = originalType;
	}

	public String getSetUrl() {
		return setUrl;
	}

	public void setSetUrl(String setUrl) {
		this.setUrl = setUrl;
	}

	public String getLmodify() {
		return lmodify;
	}

	public void setLmodify(String lmodify) {
		this.lmodify = lmodify;
	}

	public void readFields(DataInput in) throws IOException {
   		setId = in.readUTF();
		setName = in.readUTF();
		topicId = in.readUTF();
		createTime = in.readUTF();
		imgSum= in.readUTF();
		cover= in.readUTF();
		userId= in.readUTF();
		postId= in.readUTF();
		source= in.readUTF();
		reporter= in.readUTF();
		nickName= in.readUTF();
		keyWord= in.readUTF();
		firstPublish= in.readUTF();
		clientSetName= in.readUTF();
		originalAuthor= in.readUTF();
		originalType= in.readUTF();
		setUrl= in.readUTF();
    	channel  = in.readUTF();
        editor = in.readUTF();
		lmodify= in.readUTF();
		
      }

    public void write(DataOutput out) throws IOException {

    	HadoopUtils.writeString(out, setId);
       	HadoopUtils.writeString(out, setName);
    	HadoopUtils.writeString(out, topicId);
    	HadoopUtils.writeString(out, createTime);
    	HadoopUtils.writeString(out, imgSum);
    	HadoopUtils.writeString(out, cover);
    	HadoopUtils.writeString(out, userId);
    	HadoopUtils.writeString(out, postId);
    	HadoopUtils.writeString(out, source);
    	HadoopUtils.writeString(out, reporter);
    	HadoopUtils.writeString(out, nickName);
    	HadoopUtils.writeString(out, keyWord);
    	HadoopUtils.writeString(out, firstPublish);
    	HadoopUtils.writeString(out, clientSetName);
    	HadoopUtils.writeString(out, originalAuthor);
    	HadoopUtils.writeString(out, originalType);
    	HadoopUtils.writeString(out, setUrl);
    	HadoopUtils.writeString(out, channel);
    	HadoopUtils.writeString(out, editor);
    	HadoopUtils.writeString(out, lmodify);
    
    }

  
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(setId).append("\t").append(setName).append("\t").append(topicId).append("\t").append(createTime)
    	.append("\t").append(imgSum).append("\t").append(cover).append("\t").append(userId)
    	.append("\t").append(postId).append("\t").append(source).append("\t").append(reporter)
    	.append("\t").append(nickName).append("\t").append(keyWord).append("\t").append(firstPublish)
    	.append("\t").append(clientSetName).append("\t").append(originalAuthor).append("\t").append(originalType)
    	.append("\t").append(setUrl).append("\t").append(channel).append("\t").append(editor).append("\t").append(lmodify);
        return  sb.toString();
    }
}
