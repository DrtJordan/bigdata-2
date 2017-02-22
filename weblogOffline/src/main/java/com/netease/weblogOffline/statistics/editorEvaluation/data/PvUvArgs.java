package com.netease.weblogOffline.statistics.editorEvaluation.data;

public class PvUvArgs {
	private String key = null;
	private String uid = null;
	private String group = null;// StatisticsIndicator_Platform_Value 所有group都需要三部分组成，缺省value部分用ShareBackChannel.all补齐
	private long count = 1l;
	
	public PvUvArgs() {}
	
	public PvUvArgs(String key, String uid) {
		this(key,uid,null,1l);
	}
	
	public PvUvArgs(String key, String uid, long count) {
		this(key,uid,null,count);
	}
	
	public PvUvArgs(String key, String uid, String group) {
		this(key,uid,group,1l);
	}
	
	public PvUvArgs(String key, String uid, String group, long count) {
		this.key = key;
		this.uid = uid;
		this.group = group;
		this.count = count;
	}
	
	@Override
	public String toString(){
		return this.key + "," + this.uid + "," + this.group + "," + this.count;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
}
