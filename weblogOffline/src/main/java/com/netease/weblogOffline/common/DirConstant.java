package com.netease.weblogOffline.common;

import java.io.File;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.netease.weblogOffline.utils.ConfigUtils;


public class DirConstant {
    
    private static Logger LOG = Logger.getLogger(DirConstant.class);
    
    public static String DIR_PREFIX;
    
    static{
        try {
            String configDir = DirConstant.class.getResource(".").getFile();
            CompositeConfiguration parseXmlConfig = ConfigUtils.parseXmlConfig(new File(configDir), new String[]{"dir_base_config.xml"});
            DIR_PREFIX = parseXmlConfig.getString("dirPrefix");
        } catch (ConfigurationException e) {
            LOG.error(e);
            DIR_PREFIX="";
        }
        LOG.info("DIR_PREFIX=" + DIR_PREFIX);
    } 
    
    public static final String DEBUG_DIR = "/debug/";
    
    //日志
    //章鱼日志
    public static final String ZY_LOG = DIR_PREFIX + "163com_zy/";
    //weblog日志
    public static final String WEBLOG_LOG = DIR_PREFIX + "163com/";
    //移动日志
//  public static final String MOBILE_LOG = "/user/portal/ODM/SDK/ODM_SDK_MOBILE_LOG/";
     public static final String MOBILE_LOG = "/user/mobilestat/rawlog/";

    public static final String MOBILE_HIVELOG = "/user/mobilestat/formatlog/event_detail/";
    
    //原始日志层
    public static final String COMMON_DATA_DIR = DIR_PREFIX + "commonData/";
    //跟帖原始数据
  //  public static final String GEN_TIE_INFO = COMMON_DATA_DIR + "genTieLog/";
    public static final String GEN_TIE_INFO = COMMON_DATA_DIR + "genTieLogNew/"; 
    //cms给的精品原创内容数据增量
    public static final String YC_INFO_INCR = COMMON_DATA_DIR + "originalContentIncr/";
    //精品原创内容数据全量
    public static final String YC_INFO_ALL = COMMON_DATA_DIR + "originalContentAll/";
    

    
    
    //中间层数据
    public static final String WEBLOG_MIDLAYER_DIR = DIR_PREFIX + "weblog/midLayer/";
    
    //统计数据
    
    public static final String WEBLOG_STATISTICS_DIR = DIR_PREFIX + "weblog/statistics/";
    //weblogFilter日志
    public static final String WEBLOG_FilterLOG =COMMON_DATA_DIR+"weblog/";
    //zylogFilter日志
	public static final String ZYLOG_FilterLOG =COMMON_DATA_DIR+"devilfishLog/";
    
    public static final String WEBLOG_STATISTICS_TEMP_DIR = WEBLOG_STATISTICS_DIR + "temp/";

    public static final String WEBLOG_STATISTICS_TODC_DIR = WEBLOG_STATISTICS_DIR + "result_toDC/";

    public static final String WEBLOG_STATISTICS_OTHER_DIR = WEBLOG_STATISTICS_DIR + "result_other/";
    //小编考核
    public static final String WEBLOG_STATISTICS_EDITOR_EVALUATION = WEBLOG_STATISTICS_DIR + "editorEvaluation/";


    public static final String WEBLOG_STATISTICS_PREFIX = "weblog_";
    
    
    //媒体数据
    public static final String MEDIA_COMPANY = DIR_PREFIX + "media/mediaCompany/";
    public static final String URL_MEDIA_INCR = COMMON_DATA_DIR + "articleIncr_web/";
    public static final String URL_MEDIA_ALL = COMMON_DATA_DIR + "articleAll_web/";

    //3g数据
    public static final String LOG_OF_3G_INCR = COMMON_DATA_DIR + "contentIncr_3g/";    
    public static final String LOG_OF_3G_ALL = COMMON_DATA_DIR + "contentAll_3g/";
    
    public static final String LOG_OF_3G_EDITOR = COMMON_DATA_DIR + "editor_3g/";
    //gentieBase
    public static final String GENTIE_BASE_INCR = COMMON_DATA_DIR + "genTieBaseIncr/";
    public static final String GENTIE_BASE_ALL = COMMON_DATA_DIR + "genTieBaseAll/";
    
    //photoSet
    public static final String PHOTOSET_INCR = COMMON_DATA_DIR + "photoSetIncr/";
    public static final String PHOTOSET_ALL = COMMON_DATA_DIR + "photoSetAll/";
    
    //article
    public static final String ARTICLE_INCR = COMMON_DATA_DIR + "articleIncr/";
    public static final String ARTICLE_ALL = COMMON_DATA_DIR + "articleAll/";
    
    //special
    public static final String SPECIAL_INCR = COMMON_DATA_DIR + "specialIncr/";
    public static final String SPECIAL_ALL = COMMON_DATA_DIR + "specialAll/";
    
    //vidio
    public static final String VIDIO_INCR = COMMON_DATA_DIR + "vidioIncr/";
    public static final String VIDIO_ALL = COMMON_DATA_DIR + "vidioAll/";
    //订阅
    public static final String DYDATA_INCR = COMMON_DATA_DIR + "dyDataIncr/";
    public static final String DYDATA_ALL = COMMON_DATA_DIR + "dyDataAll/";
    
    //gentieVote
    public static final String GENTIE_VOTE_LOG = COMMON_DATA_DIR + "genTieVoteLog/";



    //中间层数据
    public static final String MEDIA_MIDLAYER_DIR = DIR_PREFIX + "media/midLayer/";
    //统计数据
    public static final String MEDIA_STATISTICS_DIR = DIR_PREFIX + "media/statistics/";
    public static final String MEDIA_STATISTICS_TEMP_DIR = MEDIA_STATISTICS_DIR + "temp/";


    //路径数据
    //中间层数据
    public static final String PATH_TEMP_DIR = DIR_PREFIX + "path/temp/";
    public static final String PATH_MIDLAYER_DIR = DIR_PREFIX + "path/midLayer/";
    //统计数据
    public static final String PATH_STATISTICS_DIR = DIR_PREFIX + "path/statistics/";

    //BIGDATA HOUSE
    public static final String HOUSE_MIDLAYER =DIR_PREFIX+"midLayer/";
    
	public static final String HOUSE_WEBLOG_MAP = HOUSE_MIDLAYER+"weblogToMap/";
	public static final String HOUSE_ZYLOG_MAP = HOUSE_MIDLAYER+"devilfishToMap/";
    public static final String HOUSE_YC_INFO_ALL = HOUSE_MIDLAYER+"originalContentAll/";
    public static final String HOUSE_URL_MEDIA_ALL =HOUSE_MIDLAYER+"articleAll_web/";
	
    
	public static final String HOUSE_DAILY_URL_PVUV_SHAREBACK_TEMP = HOUSE_MIDLAYER+"dailyUrlPvUvShareBackTemp/";
	public static final String HOUSE_DAILY_URL_PVUV_SHAREBACK = HOUSE_MIDLAYER+"dailyUrlPvUvShareBack/";
	
	public static final String HOUSE_DAILY_URL_INFO = HOUSE_MIDLAYER+"dailyUrlInfoVector/";

	public static final String HOUSE_DAILY_URL_GENTIE_TEMP = HOUSE_MIDLAYER+"dailyUrlGenTieCountAndUserCountTemp/";

	public static final String HOUSE_DAILY_URL_GENTIE = HOUSE_MIDLAYER+"dailyUrlGenTieCountAndUserCount/";

	public static final String HOUSE_DAILY_URL_YC = HOUSE_MIDLAYER+"dailyUrlOriginalContent/";

	public static final String HOUSE_DAILY_URL_MEDIA = HOUSE_MIDLAYER+"dailyArticleSource/";


}



