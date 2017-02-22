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


public class LongContent_3gWritable implements Writable {

    private long first;
    private Content_3gWritable second = new Content_3gWritable();
    
    
    public LongContent_3gWritable() {    }
    
    public LongContent_3gWritable(LongContent_3gWritable one) {
        this.first = one.first;
        this.second = one.second;
    }
    
    /**
     * @param first
     * @param second
     */
    public LongContent_3gWritable(long first, Content_3gWritable second) {
        super();
        this.first = first;
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public Content_3gWritable getSecond() {
        return second;
    }

    public void setSecond(Content_3gWritable second) {
        this.second = second;
    }
    
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second.readFields(in);

      }

    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        second.write(out);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof LongContent_3gWritable)){
            return false;
        }
        LongContent_3gWritable other = (LongContent_3gWritable)o;
        return this.first == other.getFirst() && this.second.equals(other.getSecond());
    }

   
    @Override
    public int hashCode(){
        return Long.toString(this.first).hashCode() ^ this.second.hashCode() + 1;
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

}
