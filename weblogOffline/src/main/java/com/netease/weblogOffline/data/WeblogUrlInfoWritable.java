package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.netease.weblogCommon.utils.TextUtils;
import com.netease.weblogOffline.utils.HadoopUtils;

	//新增项时，更新keySize大小，必须按顺序追加到末尾
public class WeblogUrlInfoWritable implements WritableComparable<WeblogUrlInfoWritable> {
		//所有有效数据项个数，不包括该项本身
		private int keySize = 17;
		
		public static void main(String[] args) {
			int count = 10;
			while(count-- > 0){
				System.out.println(count);
			}
		}
		
		private String uuid = "uuid";
		private String sid = "sid";
		private String pgr = "pgr";
		private String url = "url";
		private String urldomain = "urldomain";
		private String ref = "ref";
		private String refdomain = "refdomain";
		private String entry = "entry";
		private String dt = "dt";
		private String utime = "utime";
		private String time = "time";
		private String project = "project";
		private String prev_pgr = "prev_pgr";
		private String unew = "unew";
		private String title1 = "title1";
		private String title2 = "title2";
		private String title3 = "title3";
		
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

		public String getPgr() {
			return pgr;
		}

		public void setPgr(String pgr) {
			this.pgr = pgr;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getUrldomain() {
			return urldomain;
		}

		public void setUrldomain(String urldomain) {
			this.urldomain = urldomain;
		}

		public String getRef() {
			return ref;
		}

		public void setRef(String ref) {
			this.ref = ref;
		}

		public String getRefdomain() {
			return refdomain;
		}

		public void setRefdomain(String refdomain) {
			this.refdomain = refdomain;
		}

		public String getEntry() {
			return entry;
		}

		public void setEntry(String entry) {
			this.entry = entry;
		}

		public String getDt() {
			return dt;
		}

		public void setDt(String dt) {
			this.dt = dt;
		}

		public String getUtime() {
			return utime;
		}

		public void setUtime(String utime) {
			this.utime = utime;
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

		public String getProject() {
			return project;
		}

		public void setProject(String project) {
			this.project = project;
		}

		public String getPrev_pgr() {
			return prev_pgr;
		}

		public void setPrev_pgr(String prev_pgr) {
			this.prev_pgr = prev_pgr;
		}

		public String getUnew() {
			return unew;
		}

		public void setUnew(String unew) {
			this.unew = unew;
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
				this.pgr = in.readUTF();
			}
			if(count-- > 0){
				this.url = in.readUTF();
			}
			if(count-- > 0){
				this.urldomain = in.readUTF();
			}
			if(count-- > 0){
				this.ref = in.readUTF();
			}
			if(count-- > 0){
				this.refdomain = in.readUTF();
			}
			if(count-- > 0){
				this.entry = in.readUTF();
			}
			if(count-- > 0){
				this.dt = in.readUTF();
			}
			if(count-- > 0){
				this.utime = in.readUTF();
			}
			
			if(count-- > 0){
				this.time = in.readUTF();
			}
			if(count-- > 0){
				this.project = in.readUTF();
			}
			if(count-- > 0){
				this.prev_pgr = in.readUTF();
			}
			if(count-- > 0){
				this.unew = in.readUTF();
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
			HadoopUtils.writeString(out, this.pgr);
			HadoopUtils.writeString(out, this.url);
			HadoopUtils.writeString(out, this.urldomain);
			HadoopUtils.writeString(out, this.ref);
			HadoopUtils.writeString(out, this.refdomain);
			HadoopUtils.writeString(out, this.entry);
			HadoopUtils.writeString(out, this.dt);
			HadoopUtils.writeString(out, this.utime);
			HadoopUtils.writeString(out, this.time);
			HadoopUtils.writeString(out, this.project);
			HadoopUtils.writeString(out, this.prev_pgr);
			HadoopUtils.writeString(out, this.unew);
			HadoopUtils.writeString(out, this.title1);
			HadoopUtils.writeString(out, this.title2);
			HadoopUtils.writeString(out, this.title3);
		}
		
		@Override
		public boolean equals(Object o){
			return o instanceof WeblogUrlInfoWritable ? compareTo((WeblogUrlInfoWritable) o) == 0 : false;
		}

		@Override
		public int compareTo(WeblogUrlInfoWritable that) {
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
			
			res = TextUtils.strCompare(this.pgr, that.pgr);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.url, that.url);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.urldomain, that.urldomain);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.ref, that.ref);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.refdomain, that.refdomain);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.entry, that.entry);
			if(res != 0){
				return res;
			}
			
			res = TextUtils.strCompare(this.dt, that.dt);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.utime, that.utime);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.time, that.time);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.project, that.project);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.prev_pgr, that.prev_pgr);
			if(res != 0){
				return res;
			}
			res = TextUtils.strCompare(this.unew, that.unew);
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
			return "[uuid=" + uuid + ", sid=" + sid + ", pgr="
					+ pgr + ", url=" + url + ", urldomain=" + urldomain
					+ ", ref=" + ref + ", refdomain=" + refdomain + ", entry="
					+ entry + ", dt=" + dt + ", utime=" + utime + ", time="
					+ time + ", project=" + project + ", prev_pgr=" + prev_pgr
					+ ", unew=" + unew + ", title1=" + title1 + ", title2="
					+ title2 + ", title3=" + title3 + "]";
		}

	}


