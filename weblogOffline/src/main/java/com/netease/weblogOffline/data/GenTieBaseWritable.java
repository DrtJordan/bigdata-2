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

import org.apache.hadoop.io.WritableComparable;

import com.netease.weblogOffline.utils.HadoopUtils;


public class GenTieBaseWritable implements WritableComparable<GenTieBaseWritable> {


    private String url;
    private String pdocid;

    private String docid;


    public GenTieBaseWritable() {    }

    public GenTieBaseWritable(GenTieBaseWritable one) {
        this.url = one.url;
        this.pdocid = one.pdocid;
        this.docid = one.docid;
    }

    /**
     * @param first
     * @param second
     */
    public GenTieBaseWritable(String url, String pdocid, String docid) {
        super();
        this.url = url;
        this.pdocid = pdocid;
        this.docid = docid;
    }


    public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
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

	public void readFields(DataInput in) throws IOException {
		url = in.readUTF();
		pdocid = in.readUTF();
		docid = in.readUTF();
      }

    public void write(DataOutput out) throws IOException {
    	HadoopUtils.writeString(out, url);
    	HadoopUtils.writeString(out, pdocid);
    	HadoopUtils.writeString(out, docid);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof GenTieBaseWritable)){
            return false;
        }
        GenTieBaseWritable other = (GenTieBaseWritable)o;
        return this.url.equals(other.getUrl())  && this.pdocid.equals(other.getPdocid())&& this.docid.equals(other.getDocid());
    }

    public int compareTo(GenTieBaseWritable o) {
        GenTieBaseWritable that = (GenTieBaseWritable)o;
        if(this.url.compareTo(that.getUrl())!=0){
            return this.url.compareTo(that.getUrl());
        }
        if(this.pdocid.compareTo(that.getPdocid())!=0){
            return this.pdocid.compareTo(that.getPdocid());
        }


        return this.docid.compareTo(that.getDocid());
    }
    
    @Override
    public int hashCode(){
        return this.url.hashCode() ^ this.pdocid.hashCode() ^ this.docid.hashCode() + 1;
    }

    @Override
    public String toString() {
        return url + "," + pdocid + "," + docid;
    }

}
