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


public class ArticleWritable implements Writable {



	private String docId ;//DocID　 
	private String articleUrl ;// 文章URL 
	private String combineGentie ;//合并跟帖（被合并文章的DocID）
	private String channel ;// 所属频道 　 
	private String cloumn ;//所属栏目　 
	private String source ;//来源　 
	private String sourceUrl ;//源url
	private String title ;//标题（文章页标题） 
	private String keyWord ;//关键字
	private String reporter ;//记者 
	private String sourceTag;//原创标记
	private String originalAuthor ;//原创作者
	private String publishTime ;//发布时间
	private String editor ;//责编（工作代码）
	private String lmodify ;//最后一次修改时间
	


    public ArticleWritable() {    }

    public ArticleWritable(ArticleWritable one) {

    	this.docId =one.docId; 
    	this.articleUrl =one.articleUrl; 
       	this.combineGentie =one.combineGentie; 
    	this.channel  = one.channel;
    	this.cloumn  = one.cloumn;
    	this.source =one.source; 
    	this.sourceUrl =one.sourceUrl; 
     	this.title =one.title; 
     	this.keyWord =one.keyWord;  	
    	this.reporter =one.reporter;
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
    public ArticleWritable(
    		String docId, 
    		String articleUrl,
    		String combineGentie,
    		String channel,
    		String cloumn,
    		String source,
    		String sourceUrl,
    		String title,
    		String keyWord,	
    		String reporter,
    		String sourceTag,
    		String originalAuthor,   	
    		String publishTime,
    		String editor,
    		String lmodify) {

    	this.docId =docId; 
    	this.articleUrl =articleUrl; 
       	this.combineGentie =combineGentie; 
    	this.channel  = channel;
    	this.cloumn  = cloumn;
    	this.source =source; 
    	this.sourceUrl =sourceUrl; 
     	this.title =title; 
     	this.keyWord =keyWord;  	
    	this.reporter =reporter;
       	this.sourceTag =sourceTag; 
    	this.originalAuthor =originalAuthor;     	
       	this.publishTime =publishTime; 
       	this.editor =editor; 
      	this.lmodify =lmodify; 
    }



	

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getArticleUrl() {
		return articleUrl;
	}

	public void setArticleUrl(String articleUrl) {
		this.articleUrl = articleUrl;
	}

	public String getCombineGentie() {
		return combineGentie;
	}

	public void setCombineGentie(String combineGentie) {
		this.combineGentie = combineGentie;
	}

	public String getCloumn() {
		return cloumn;
	}

	public void setCloumn(String cloumn) {
		this.cloumn = cloumn;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSourceTag() {
		return sourceTag;
	}

	public void setSourceTag(String sourceTag) {
		this.sourceTag = sourceTag;
	}

	public String getPublishTime() {
		return publishTime;
	}

	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
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



	public String getKeyWord() {
		return keyWord;
	}

	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}



	public String getOriginalAuthor() {
		return originalAuthor;
	}

	public void setOriginalAuthor(String originalAuthor) {
		this.originalAuthor = originalAuthor;
	}


	public String getLmodify() {
		return lmodify;
	}

	public void setLmodify(String lmodify) {
		this.lmodify = lmodify;
	}

	public void readFields(DataInput in) throws IOException {
		
		docId = in.readUTF() ; 
    	articleUrl = in.readUTF(); 
       	combineGentie = in.readUTF(); 
    	channel = in.readUTF();
    	cloumn = in.readUTF();
    	source = in.readUTF(); 
    	sourceUrl = in.readUTF(); 
     	title = in.readUTF(); 
     	keyWord = in.readUTF();  	
    	reporter = in.readUTF();
       	sourceTag = in.readUTF(); 
    	originalAuthor = in.readUTF();     	
        publishTime = in.readUTF(); 
        editor = in.readUTF(); 
      	lmodify = in.readUTF(); 
		
      }

    public void write(DataOutput out) throws IOException {

    	HadoopUtils.writeString(out, docId);
       	HadoopUtils.writeString(out, articleUrl);
    	HadoopUtils.writeString(out, combineGentie);
    	HadoopUtils.writeString(out, channel);
    	HadoopUtils.writeString(out, cloumn);
    	HadoopUtils.writeString(out, source);
    	HadoopUtils.writeString(out, sourceUrl);
    	HadoopUtils.writeString(out, title);
    	HadoopUtils.writeString(out, keyWord);
    	HadoopUtils.writeString(out, reporter);
    	HadoopUtils.writeString(out, sourceTag);
    	HadoopUtils.writeString(out, originalAuthor);
    	HadoopUtils.writeString(out, publishTime);
    	HadoopUtils.writeString(out, editor);
    	HadoopUtils.writeString(out, lmodify);

    
    }

  
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(docId).append("\t").append(articleUrl).append("\t").append(combineGentie).append("\t").append(channel)
    	.append("\t").append(cloumn).append("\t").append(source).append("\t").append(sourceUrl)
    	.append("\t").append(title).append("\t").append(keyWord).append("\t").append(reporter)
    	.append("\t").append(sourceTag).append("\t").append(originalAuthor).append("\t").append(publishTime)
    	.append("\t").append(editor).append("\t").append(lmodify);
        return  sb.toString();
    }
}
