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


/**
 *
 * @author mengyan
 *
 */
public class LongLongWritable implements WritableComparable<LongLongWritable> {

    private long first;
    private long second;
    
    
    public LongLongWritable() {    }
    
    public LongLongWritable(LongLongWritable one) {
        this.first = one.first;
        this.second = one.second;
    }
    
    /**
     * @param first
     * @param second
     */
    public LongLongWritable(long first, long second) {
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

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }
    
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
      }

    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof LongLongWritable))
            return false;
        LongLongWritable other = (LongLongWritable)o;
        return this.first == other.getFirst() && this.second == other.getSecond();
    }

    public int hashCode() {
        return (int)(first ^ (second + 1));
    }

    public int compareTo(LongLongWritable o) {
        
        LongLongWritable that = (LongLongWritable)o;
        return that.getFirst() == this.first ?
                that.getSecond() == this.second ? 
                        0 : that.getSecond() > this.second ? -1 : 1
                : that.getFirst() > this.first ? -1 : 1;
                
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

}
