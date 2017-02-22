package com.netease.weblogOffline.statistics.bigdatahouse;

import java.util.HashMap;


import com.netease.weblogOffline.data.HashMapStringStringWritable;


public class DailyUrlInfoUtils {
	
	
	public static void main(String[] args){

	}
	public static final String defNullStr = "(null)";
	public static final String defNullInt = "0";
	
	private static final String[] strColumns = {"url","type","isyc","mediasource","pure_url","pure_type","pure_isyc","pure_mediasource"};	
	private static final String[] sharebackColumns = {"shareCount_pc","backCount_pc","qzone_shareCount_pc","qzone_backCount_pc","weibo_shareCount_pc","weibo_backCount_pc","renren_shareCount_pc","renren_backCount_pc","weibo163_shareCount_pc","weibo163_backCount_pc","youdao_shareCount_pc","youdao_backCount_pc","yixin_shareCount_pc","yixin_backCount_pc","weixin_shareCount_pc","weixin_backCount_pc","lofter_shareCount_pc","lofter_backCount_pc","hot163_shareCount_pc","hot163_backCount_pc",
		"pure_shareCount_pc","pure_backCount_pc","pure_qzone_shareCount_pc","pure_qzone_backCount_pc","pure_weibo_shareCount_pc","pure_weibo_backCount_pc","pure_renren_shareCount_pc","pure_renren_backCount_pc","pure_weibo163_shareCount_pc","pure_weibo163_backCount_pc","pure_youdao_shareCount_pc","pure_youdao_backCount_pc","pure_yixin_shareCount_pc","pure_yixin_backCount_pc","pure_weixin_shareCount_pc","pure_weixin_backCount_pc","pure_lofter_shareCount_pc","pure_lofter_backCount_pc","pure_hot163_shareCount_pc","pure_hot163_backCount_pc"};
	private static final String[] pvuvColumns = {"pv_pc","uv_pc","pure_pv_pc","pure_uv_pc"};
	private static final String[] gentieColumns = {"gentieCount","ph_gentieCount","wb_gentieCount","zq_gentieCount","3g_gentieCount","uc_gentieCount","qq_gentieCount","cc_gentieCount","jn_gentieCount","other_gentieCount",
		"pure_gentieCount","pure_gentieUserCount","pure_ph_gentieCount","pure_wb_gentieCount","pure_zq_gentieCount","pure_3g_gentieCount","pure_uc_gentieCount","pure_qq_gentieCount","pure_cc_gentieCount","pure_jn_gentieCount","pure_other_gentieCount"
		,"gentieUserCount","ph_gentieUserCount","wb_gentieUserCount","zq_gentieUserCount","3g_gentieUserCount","uc_gentieUserCount","qq_gentieUserCount","cc_gentieUserCount","jn_gentieUserCount","other_gentieUserCount",
		"pure_gentieUserCount","pure_ph_gentieUserCount","pure_wb_gentieUserCount","pure_zq_gentieUserCount","pure_3g_gentieUserCount","pure_uc_gentieUserCount","pure_qq_gentieUserCount","pure_cc_gentieUserCount","pure_jn_gentieUserCount","pure_other_gentieUserCount"};

	private static final String[] gentieSourceColumns ={"ph","wb","zq","3g","uc","qq","cc","jn"};
	
	private static final String[] urlColumns ={"url","type","isyc","mediasource",
		"pv_pc","uv_pc",
		"shareCount_pc","backCount_pc","qzone_shareCount_pc","qzone_backCount_pc","weibo_shareCount_pc","weibo_backCount_pc","renren_shareCount_pc","renren_backCount_pc","weibo163_shareCount_pc","weibo163_backCount_pc","youdao_shareCount_pc","youdao_backCount_pc","yixin_shareCount_pc","yixin_backCount_pc","weixin_shareCount_pc","weixin_backCount_pc","lofter_shareCount_pc","lofter_backCount_pc","hot163_shareCount_pc","hot163_backCount_pc",
		"gentieCount","ph_gentieCount","wb_gentieCount","zq_gentieCount","3g_gentieCount","uc_gentieCount","qq_gentieCount","cc_gentieCount","jn_gentieCount","other_gentieCount",
		"gentieUserCount","ph_gentieUserCount","wb_gentieUserCount","zq_gentieUserCount","3g_gentieUserCount","uc_gentieUserCount","qq_gentieUserCount","cc_gentieUserCount","jn_gentieUserCount","other_gentieUserCount"};

	private static final String[] pureurlColumns ={"pure_url","pure_type","pure_isyc","pure_mediasource",
		"pure_pv_pc","pure_uv_pc",
		"pure_shareCount_pc","pure_backCount_pc","pure_qzone_shareCount_pc","pure_qzone_backCount_pc","pure_weibo_shareCount_pc","pure_weibo_backCount_pc","pure_renren_shareCount_pc","pure_renren_backCount_pc","pure_weibo163_shareCount_pc","pure_weibo163_backCount_pc","pure_youdao_shareCount_pc","pure_youdao_backCount_pc","pure_yixin_shareCount_pc","pure_yixin_backCount_pc","pure_weixin_shareCount_pc","pure_weixin_backCount_pc","pure_lofter_shareCount_pc","pure_lofter_backCount_pc","pure_hot163_shareCount_pc","pure_hot163_backCount_pc",
		"pure_gentieCount","pure_ph_gentieCount","pure_wb_gentieCount","pure_zq_gentieCount","pure_3g_gentieCount","pure_uc_gentieCount","pure_qq_gentieCount","pure_cc_gentieCount","pure_jn_gentieCount","pure_other_gentieCount",
		"pure_gentieUserCount","pure_ph_gentieUserCount","pure_wb_gentieUserCount","pure_zq_gentieUserCount","pure_3g_gentieUserCount","pure_uc_gentieUserCount","pure_qq_gentieUserCount","pure_cc_gentieUserCount","pure_jn_gentieUserCount","pure_other_gentieUserCount"};
	
	public static String[] getUrlcolumns() {
		return urlColumns;
	}

	public static String[] getPureurlcolumns() {
		return pureurlColumns;
	}
	public static String[] getGentieSourcecolumns() {
		return gentieSourceColumns;
	}
	
	public static String getGentieSource(String source) {
		for (String s :getGentieSourcecolumns()){
			if (s.equals(source)){
				return s ;
			}
		}
		return "other";
	}
	
	
	

	//为了效率，这里没限制数组内容不能修改，为了程序的正确行，请不要修改获取的数组
	public static String[] getStrColumns(){
		return strColumns;
	}
	

	
	public static String[] getStrcolumns() {
		return strColumns;
	}



	public static String[] getSharebackcolumns() {
		return sharebackColumns;
	}



	public static String[] getPvuvcolumns() {
		return pvuvColumns;
	}



	public static String[] getGentiecolumns() {
		return gentieColumns;
	}



	public static HashMap<String, String> buildUrlKVMap(HashMapStringStringWritable lineMap){

		
		HashMap<String, String> res = new HashMap<String, String>();
		

		String[] urlColumns =getUrlcolumns();


		for(String column:urlColumns){
			res.put(column, lineMap.getHm().get(column)==null?defNullStr:lineMap.getHm().get(column));
		}
		
		return res;
	}
	
	public static HashMap<String, String> buildPureUrlKVMap(HashMapStringStringWritable lineMap){

		
		HashMap<String, String> res = new HashMap<String, String>();
		

		String[] pureUrlColumns =getPureurlcolumns();

		for(String column:pureUrlColumns){
			res.put(column, lineMap.getHm().get(column)==null?defNullStr:lineMap.getHm().get(column));
		}
	
		return res;
	}
	
}
