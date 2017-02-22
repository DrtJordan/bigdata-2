/**
 * @(#)LongLongWritable.java, 2015-06-07. 
 * 
 * Copyright 2015 Netease, Inc. All rights reserved.
 * 
 */
package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class CountOfBouncePageWritable implements WritableComparable<CountOfBouncePageWritable> {

    private int  count =0;
    
    
    public CountOfBouncePageWritable() {    }
    
    public CountOfBouncePageWritable(CountOfBouncePageWritable one) {
        this.count = one.count;
    }
    
    /**
     * @param count
     *
     */
    public CountOfBouncePageWritable(int count, String second) {
        super();
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

   


    
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();

      }

    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
    }

    public boolean equals(Object o) {
        if (o == null && !(o instanceof CountOfBouncePageWritable)){
            return false;
        }
        CountOfBouncePageWritable other = (CountOfBouncePageWritable)o;
        return this.count == other.getCount();
    }

    public int compareTo(CountOfBouncePageWritable o) {
        CountOfBouncePageWritable that = (CountOfBouncePageWritable)o;
        return this.count>that.getCount()?-1:this.count<that.getCount()?1:0;
    }
        

    
    @Override
    public int hashCode(){
        return this.count+ 1;
    }

    @Override
    public String toString() {
        return count+"";
    }

}
