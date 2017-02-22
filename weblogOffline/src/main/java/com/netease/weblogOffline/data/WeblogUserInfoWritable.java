package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

//新增项时，更新keySize大小，必须按顺序追加到末尾
public class WeblogUserInfoWritable implements WritableComparable<WeblogUserInfoWritable> {
	//所有有效数据项个数，不包括该项本身
	private int keySize = 8;
	
	private String uuid;
	private String sid;
	private String email;
	private String dt;
	private String ip;
	private String title1;
	private String title2;
	private String title3;
	
	public int getKeySize() {
		return keySize;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getDt() {
		return dt;
	}

	public void setDt(String dt) {
		this.dt = dt;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getTitle1() {
		return title1;
	}

	public void setTitle1(String title1) {
		this.title1 = title1;
	}

	public String getTitle2() {
		return title2;
	}

	public void setTitle2(String title2) {
		this.title2 = title2;
	}

	public String getTitle3() {
		return title3;
	}

	public void setTitle3(String title3) {
		this.title3 = title3;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.keySize = in.readInt();
		int count = this.keySize;
		if(count-- > 0){
			this.uuid = in.readUTF();
		}
		
		if(count-- > 0){
			this.sid = in.readUTF();
		}
		
		if(count-- > 0){
			this.email = in.readUTF();
		}
		
		if(count-- > 0){
			this.dt = in.readUTF();
		}
		
		if(count-- > 0){
			this.ip = in.readUTF();
		}
		
		if(count-- > 0){
			this.title1 = in.readUTF();
		}
		
		if(count-- > 0){
			this.title2 = in.readUTF();
		}
		
		if(count-- > 0){
			this.title3 = in.readUTF();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.keySize);
		HadoopUtils.writeString(out, this.uuid);
		HadoopUtils.writeString(out, this.sid);
		HadoopUtils.writeString(out, this.email);
		HadoopUtils.writeString(out, this.dt);
		HadoopUtils.writeString(out, this.ip);
		HadoopUtils.writeString(out, this.title1);
		HadoopUtils.writeString(out, this.title2);
		HadoopUtils.writeString(out, this.title3);
	}
	
	@Override
	public boolean equals(Object o){
		return o instanceof WeblogUserInfoWritable ? compareTo((WeblogUserInfoWritable) o) == 0 : false;
	}

	@Override
	public int compareTo(WeblogUserInfoWritable that) {
		if(null == that){
			return 1;
		}
		
		int res = 0;
		
		res = this.keySize > that.keySize ? 1 : (this.keySize == that.keySize ? 0 : -1);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.uuid, that.uuid);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.sid, that.sid);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.email, that.email);
		if(res != 0){
			return res;
		}
		
		
		res = TextUtils.strCompare(this.dt, that.dt);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.ip, that.ip);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.title1, that.title1);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.title2, that.title2);
		if(res != 0){
			return res;
		}
		
		res = TextUtils.strCompare(this.title3, that.title3);
		if(res != 0){
			return res;
		}
		
		return res;
	}
	
	@Override
	public int hashCode(){
		return toString().hashCode();
	}

	@Override
	public String toString() {
		return "[uuid=" + uuid + ", sid=" + sid + ", email="
				+ email + ", dt=" + dt + ", ip=" + ip + ", title1=" + title1
				+ ", title2=" + title2 + ", title3=" + title3 + "]";
	}

}
