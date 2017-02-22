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


public class LongHashMapStringStringWritable implements Writable {

    private long first;
    private HashMapStringStringWritable second = new HashMapStringStringWritable();
    
    
    public LongHashMapStringStringWritable() {    }
    
    public LongHashMapStringStringWritable(LongHashMapStringStringWritable one) {
        this.first = one.first;
        this.second.getHm().putAll(one.getSecond().getHm());
    }
    
    /**
     * @param first
     * @param second
     */
    public LongHashMapStringStringWritable(long first, HashMapStringStringWritable second) {
        super();
        this.first = first;
        this.second.getHm().putAll(second.getHm());
    }

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public HashMapStringStringWritable getSecond() {
        return second;
    }

    public void setSecond(HashMapStringStringWritable second) {
           this.second.getHm().clear();
    	   this.second.getHm().putAll(second.getHm());
    }
    
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = new HashMapStringStringWritable();
        second.readFields(in);
       
      }

    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        second.write(out);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof LongHashMapStringStringWritable)){
            return false;
        }
        LongHashMapStringStringWritable other = (LongHashMapStringStringWritable)o;
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
