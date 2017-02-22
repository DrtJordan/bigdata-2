package com.netease.weblogOffline.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ContainFileFilter implements PathFilter{
	
	private String s = null;
	
	public ContainFileFilter(String s) {
		this.s = s;
	}

	@Override
	public boolean accept(Path paramPath) {
		return null == paramPath ? false : paramPath.getName().contains(s);
	}

}
