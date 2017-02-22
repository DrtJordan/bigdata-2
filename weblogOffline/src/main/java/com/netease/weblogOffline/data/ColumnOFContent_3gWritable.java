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


public class ColumnOFContent_3gWritable implements WritableComparable<ColumnOFContent_3gWritable> {


    private String clolumnName;
    private String clolumnId;
    private String id;
    private String type;
    private String title;
    private String date;
    private String publishTime ;

//	private String lmodify ;//lmodify
	private String wight;
    


    public ColumnOFContent_3gWritable() {    }

    public ColumnOFContent_3gWritable(ColumnOFContent_3gWritable one) {
          this.clolumnName=one.clolumnName;
          this.clolumnId=one.clolumnId;

          this.id=one.id;
          this.type=one.type;
          this.title=one.title;

          this.date = one.date;
          this.publishTime = one.publishTime;
          this.wight=one.wight;
    }

    /**
     * 
     */
    public ColumnOFContent_3gWritable(String clolumnName,String clolumnId,String photoSetId,String type,String title,String date,String publishTime,String wight) {
      this.clolumnName=clolumnName;
      this.clolumnId=clolumnId;

      this.id=photoSetId;
      this.type=type;
      this.title=title;
      this.date = date;
      this.publishTime = publishTime;
      this.wight=wight;
    }



    public String getPublishTime() {
		return publishTime;
	}

	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
	}


	public String getWight() {
		return wight;
	}

	public void setWight(String wight) {
		this.wight = wight;
	}

	public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getClolumnName() {
        return clolumnName;
    }

    public void setClolumnName(String clolumnName) {
        this.clolumnName = clolumnName;
    }

    public String getClolumnId() {
        return clolumnId;
    }

    public void setClolumnId(String clolumnId) {
        this.clolumnId = clolumnId;
    }



    public String getId() {
        return id;
    }

    public void setId(String photoSetId) {
        this.id = photoSetId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


    public void readFields(DataInput in) throws IOException {
        clolumnName = in.readUTF();
        clolumnId = in.readUTF();

        id = in.readUTF();
        type = in.readUTF();
        title = in.readUTF();
        date =  in.readUTF();
        publishTime =in.readUTF();
        wight=in.readUTF();

        

      }

    public void write(DataOutput out) throws IOException {
        HadoopUtils.writeString(out, clolumnName);      
        HadoopUtils.writeString(out, clolumnId);

         HadoopUtils.writeString(out, id);      
        HadoopUtils.writeString(out, type);
        HadoopUtils.writeString(out, title);
        HadoopUtils.writeString(out, date);
        HadoopUtils.writeString(out, publishTime);

        HadoopUtils.writeString(out, wight);

    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof ColumnOFContent_3gWritable)){
            return false;
        }
        ColumnOFContent_3gWritable other = (ColumnOFContent_3gWritable)o;
        return this.clolumnName.equals(other.getClolumnName()) &&
                this.clolumnId.equals(other.getClolumnId())&& 

               this.id.equals(other.getId())&&
               this.type.equals(other.getType())&&
               this.title.equals(other.getTitle())&&
               this.publishTime.equals(other.getPublishTime())&&

               this.wight.equals(other.getWight());
    }

    public int compareTo(ColumnOFContent_3gWritable o) {
        ColumnOFContent_3gWritable that = (ColumnOFContent_3gWritable)o;
        if(this.clolumnName.compareTo(that.getClolumnName())!=0){
            return this.clolumnName.compareTo(that.getClolumnName());
        }
        if(this.clolumnId.compareTo(that.getClolumnId())!=0){
            return this.clolumnId.compareTo(that.getClolumnId());
        }

        if(this.id.compareTo(that.getId())!=0){
            return this.id.compareTo(that.getId());
        }
        if(this.type.compareTo(that.getType())!=0){
            return this.type.compareTo(that.getType());
        }
        
        if(this.title.compareTo(that.getTitle())!=0){
        	   return this.title.compareTo(that.getTitle());
        }
        
        if(this.publishTime.compareTo(that.getPublishTime())!=0){
            return this.publishTime.compareTo(that.getPublishTime());
        }



            return this.wight.compareTo(that.getWight());
        

    }

    @Override
    public int hashCode(){
        return this.clolumnName.hashCode() ^ this.clolumnId.hashCode() ^this.id.hashCode() ^ this.type.hashCode() ^ this.title.hashCode()^this.date.hashCode()^this.publishTime.hashCode()^this.wight.hashCode()+ 1;
    }

    @Override
    public String toString() {
        return clolumnName + "\t" + clolumnId + "\t" + id + "\t" + type+ "\t" + title+"\t"+date+"\t"+publishTime+"\t"+publishTime+"\t"+wight;
    }

}

