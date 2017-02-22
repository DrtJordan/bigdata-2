package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An Ordered List of Strings which can be used in a Hadoop Map/Reduce Job
 */
public final class StringTuple implements WritableComparable<StringTuple> {
  
  private List<String> tuple = Lists.newArrayList();
  
  public StringTuple() { }
  
  public StringTuple(StringTuple st) { 
	  this.tuple= st.tuple;
  }
  public StringTuple(String firstEntry) {
    add(firstEntry);
  }
  
  public StringTuple(Iterable<String> entries) {
    for (String entry : entries) {
      add(entry);
    }
  }
  
  public StringTuple(String[] entries) {
    for (String entry : entries) {
      add(entry);
    }
  }
  
  /**
   * add an entry to the end of the list
   * 
   * @param entry
   * @return true if the items get added
   */
  public boolean add(String entry) {
    return tuple.add(entry);
  }
  
  /**
   * Fetches the string at the given location
   * 
   * @param index
   * @return String value at the given location in the tuple list
   */
  public String stringAt(int index) {
    return tuple.get(index);
  }
  
  /**
   * Replaces the string at the given index with the given newString
   * 
   * @param index
   * @param newString
   * @return The previous value at that location
   */
  public String replaceAt(int index, String newString) {
    return tuple.set(index, newString);
  }
  
  /**
   * Fetch the list of entries from the tuple
   * 
   * @return a List containing the strings in the order of insertion
   */
  public List<String> getEntries() {
    return Collections.unmodifiableList(this.tuple);
  }
  
  /**
   * Returns the length of the tuple
   * 
   * @return length
   */
  public int length() {
    return this.tuple.size();
  }
  
  @Override
  public String toString() {
    return tuple.toString();
  }
  
  @Override
  public int hashCode() {
    return tuple.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    StringTuple other = (StringTuple) obj;
    if (tuple == null) {
      if (other.tuple != null) {
        return false;
      }
    } else if (!tuple.equals(other.tuple)) {
      return false;
    }
    return true;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    tuple = Lists.newArrayListWithCapacity(len);
    Text value = new Text();
    for (int i = 0; i < len; i++) {
      value.readFields(in);
      tuple.add(value.toString());
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(tuple.size());
    Text value = new Text();
    for (String entry : tuple) {
      value.set(entry);
      value.write(out);
    }
  }
  
  @Override
  public int compareTo(StringTuple otherTuple) {
    int thisLength = length();
    int otherLength = otherTuple.length();
    int min = Math.min(thisLength, otherLength);
    for (int i = 0; i < min; i++) {
      int ret = this.tuple.get(i).compareTo(otherTuple.stringAt(i));
      if (ret != 0) {
        return ret;
      }
    }
    if (thisLength < otherLength) {
      return -1;
    } else if (thisLength > otherLength) {
      return 1;
    } else {
      return 0;
    }
  }
  
}
