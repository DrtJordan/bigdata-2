package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



/** A simple pair of StringTuple. */
public final class StringTuplePair implements Writable {
  
  private  StringTuple first= new StringTuple();
  private  StringTuple second= new StringTuple();
  
  
  public StringTuplePair() {

	  }
	  
  public StringTuplePair(StringTuple first, StringTuple second) {
    this.first = first;
    this.second = second;
  }
  
  public StringTuple getFirst() {
    return first;
  }
  
  public StringTuple getSecond() {
    return second;
  }
  
  public StringTuplePair swap() {
    return new StringTuplePair(second, first);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StringTuplePair)) {
      return false;
    }
    StringTuplePair otherPair = (StringTuplePair) obj;
    return first == otherPair.getFirst() && second == otherPair.getSecond();
  }
  
  @Override
  public int hashCode() {
    int firstHash = first.hashCode();
    // Flip top and bottom 16 bits; this makes the hash function probably different
    // for (a,b) versus (b,a)
    return (firstHash >>> 16 | firstHash << 16) ^ second.hashCode();
  }
  
  @Override
  public String toString() {
    return '(' + first.toString() + ',' + second.toString() + ')';
  }

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		StringTuple first = new StringTuple();
		StringTuple second = new StringTuple();
		first.readFields(in);
		second.readFields(in);
		this.first= first;
		this.second= second;
	}

}
