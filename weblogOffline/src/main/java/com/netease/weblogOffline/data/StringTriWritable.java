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


public class StringTriWritable implements WritableComparable<StringTriWritable> {


    private String first;
    private String second;

    private String third;


    public StringTriWritable() {    }

    public StringTriWritable(StringTriWritable one) {
        this.first = one.first;
        this.second = one.second;
        this.third = one.third;
    }

    /**
     * @param first
     * @param second
     */
    public StringTriWritable(String first, String second, String third) {
        super();
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }



    public String getThird() {
        return third;
    }

    public void setThird(String third) {
        this.third = third;
    }
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readUTF();
        third = in.readUTF();
      }

    public void write(DataOutput out) throws IOException {
        if (first == null) {
            out.writeUTF("");
        } else {
            out.writeUTF(first);
        }
        if (second == null) {
            out.writeUTF("");
        } else {
            out.writeUTF(second);
        }
        if (third == null) {
            out.writeUTF("");
        } else {
            out.writeUTF(third);
        }
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof StringTriWritable)){
            return false;
        }
        StringTriWritable other = (StringTriWritable)o;
        return this.first == other.getFirst() && this.second.equals(other.getSecond())&& this.third.equals(other.getThird());
    }

    public int compareTo(StringTriWritable o) {
        StringTriWritable that = (StringTriWritable)o;
        if(this.first.compareTo(that.first)!=0){
            return this.first.compareTo(that.first);
        }
        if(this.second.compareTo(that.getSecond())!=0){
            return this.second.compareTo(that.getSecond());
        }


        return this.third.compareTo(that.getThird());
    }
    
    @Override
    public int hashCode(){
        return this.first.hashCode() ^ this.second.hashCode() ^ this.third.hashCode() + 1;
    }

    @Override
    public String toString() {
        return first + "," + second + "," + third;
    }

}
