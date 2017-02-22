
package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class ThreeStringWritable implements WritableComparable<ThreeStringWritable> {

	private   String first="";
	private   String second="";
	private   String third="";
	
	public ThreeStringWritable(String first,String  second, String  third) {

		this.first = first;
		this.second = second;
		this.third = third;
	}
	public ThreeStringWritable() {

	}
   
	public String getfirst() {
		return first;
	}
	public void setfirst(String first) {
		this.first = first;
	}
	public String getsecond() {
		return second;
	}
	public void setsecond(String second) {
		this.second = second;
	}
	public String getthird() {
		return third;
	}
	public void setthird(String third) {
		this.third = third;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.first);
		out.writeUTF(this.second);
		out.writeUTF(this.third);


	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.first=in.readUTF();
		this.second=in.readUTF();
		this.third=in.readUTF();

	}
	
	
	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
	    if (o == null && !(o instanceof ThreeStringWritable)){
            return false;
        }
	    ThreeStringWritable other = (ThreeStringWritable)o;
        return this.first .equals(other.getfirst())&& this.second.equals(other.getsecond())&&this.third.equals(other.getthird());

	}
	@Override
	public int compareTo(ThreeStringWritable cbu) {
		// TODO Auto-generated method stub
		ThreeStringWritable that = (ThreeStringWritable)cbu;
	        if(this.first.compareTo(that.first)!=0){
	            return this.first.compareTo(that.first);
	        }else if (this.second.compareTo(that.second)!=0){
	        	  return this.second.compareTo(that.second);
	        }
		return this.third.compareTo(that.third);
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
