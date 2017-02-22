/**
 * @(#)LongLongWritable.java, 2015-06-07. 
 * 
 * Copyright 2015 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class IntLongWritable implements WritableComparable<IntLongWritable> {

    private int first;
    private long second;
    
    
    public IntLongWritable() {    }
    
    public IntLongWritable(IntLongWritable one) {
        this.first = one.first;
        this.second = one.second;
    }
    
    /**
     * @param first
     * @param second
     */
    public IntLongWritable(int first, long second) {
        super();
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }
    
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readLong();
      }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeLong(second);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof IntLongWritable)){
            return false;
        }
        IntLongWritable other = (IntLongWritable)o;
        return  this.second==other.getSecond();
    }

    public int compareTo(IntLongWritable o) {
        IntLongWritable that = (IntLongWritable)o;
        return this.second>that.getSecond()?1:this.second<that.getSecond()?-1:0;
    }
    
    @Override
    public int hashCode(){
        return (int) (this.first<<16^ this.second);
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

}
