package com.netease.weblogOffline.statistics.editorEvaluation.generators;

import java.util.List;

import com.netease.weblogOffline.statistics.editorEvaluation.data.PvUvArgs;

public interface PvUvArgsGenerator {
	
	/**
	 * 解析原始日志，返回List<PvUvArgs>
	 * 输入文件格式：TextInputFormat
	 * 
	 **/
	abstract public List<PvUvArgs> execute(String line) throws Exception;

}
