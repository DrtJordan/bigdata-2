package com.netease.weblogCommon.data.enums;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 签约媒体url对应的频道（from 章鱼）
 * Created by hfchen on 2015/3/24.
 */
public enum MediaUrlRegexChannel {


    news("news",
        "^http://(bbs|war|history|news[\\d]*|discovery?|focus|media|bj|beijing|bluecross|lovegongyi|expo|jiuzai|data|world)\\.163\\.com.*,"
            + "^http://[^\\.]+\\.(bbs|bj|beijing|news|war|history)\\.163\\.com.*,"
            + "^http://(163|netease)\\.com.*,"
            + "^http://bbs[1-9]\\.news\\.163\\.com.*,"
            + "^http://all\\.163\\.com/news.*,"
            + "^http://bbs\\.netease\\.com/news.*,"
            + "^http://other\\.163\\.com/tq.*,"
            + "^http://(bbs|bbs2|post)\\.travel\\.163\\.com/*,"
            + "^http://bbs\\.(health|abroad|talk|)\\.163\\.com/.*,"
            + "^http://bbs\\.g4\\.163\\.com.*,"
            + "^http://bbs\\.163\\.com/news/.*,"
            + "^http://t\\.163\\.com/zt/news/.*,"
            + "^http://live\\.163\\.com/room/news/.*,"
            + "^http://(view|attitude)\\.163\\.com.*,"
            + "^http://www\\.163\\.com/rss/.*,"
            + "^http://news\\.tag\\.163\\.com.*,"
            + "^http://tag\\.163\\.com/news.*,"
            + "^http://tag\\.163\\.com($|/(?:[\\?#].*)?$).*,"
            + "^http://(shehui|domestic|world)\\.firefox\\.163\\.com/.*"),
    stock("stock",
        "^http://(money|stock|fund|stocksms|stock2|media|finance|stocksms|hq|emarketing|biz|biz2|biz3|biz4|minisite)\\.163\\.com.*,"
        + "^http://(bbs|comment|quote|quotes|bbs2|bbs3|my|info|mobile|media|chart|money|portfolio|2006)\\.stock\\.163\\.com.*,"
        + "^http://[^/]*\\.(finance|fund|biz|biz2|biz3|biz4|money)\\.163\\.com.*,"
        + "^http://bbs\\.163\\.com/money/.*,"
        + "^http://t\\.163\\.com/zt/money/.*,"
        + "^http://live\\.163\\.com/room/stock/.*,"
        + "^http://tag\\.163\\.com/money.*,"
        + "^http://yuedu\\.163\\.com/source.*,"
        + "^http://money\\.firefox\\.163\\.com.*"),
    tech("tech",
        "^http://(tech\\d*|it)\\.163\\.com.*,"
        + "^http://[^/]*\\.tech\\d*\\.163\\.com.*,"
        + "^http://cimg3\\.163\\.com/(feiting|kepu).*,"
        + "^http://all\\.163\\.com/(science|it).*,"
        + "^http://bbs5\\.netease\\.com/tech.*,"
        + "^http://bbs\\.163\\.com/tech/.*,"
        + "^http://t\\.163\\.com/zt/tech/.*,"
        + "^http://live\\.163\\.com/room/tech/.*,"
        + "^http://tag\\.163\\.com/tech.*,"
        + "^http://tech\\.firefox\\.163\\.com.*"),
    digi("digi",
        "^http://(digi|buy|pc|av|dp|dpshow|wm|jvc|minisite)\\.163\\.com.*,"
        + "^http://[^/]*\\.(digi|buy|jvc)\\.163\\.com.*,"
        + "^http://bbs8\\.netease\\.com/(avclub|dpclub|pcclub|wmclub|mclub).*,"
        + "^http://comment\\.(tech|digi)\\.163\\.com/.*,"
        + "^http://tech\\.163\\.com/digi/.*,"
        + "^http://product\\.tech\\.163\\.com/.*,"
        + "^http://digibbs\\.tech\\.163\\.com/.*,"
        + "^http://gotech\\.163\\.com/.*,"
        + "^http://bbs\\.163\\.com/digi/.*,"
        + "^http://hea\\.163\\.com/.*,"
        + "^http://t\\.163\\.com/zt/digi/.*,"
        + "^http://live\\.163\\.com/room/digi/.*,"
        + "^http://tech\\.163\\.com/[^\\.]*00162JVB.*"),
    sports("sports",
        "^http://([^/]+\\.)?(2012|2014)\\.163\\.com/.*,"
        + "^http://([^/]+\\.)?(sports|sport|f1|winnerway|2008|2010|2012|cbachina|2010worldcup|worldcup2010|fibawc2010)\\.163\\.com.*,"
        + "^http://(gpcfootball|euro2008|sz2011|euro2012|2014|cai)\\.163\\.com.*,"
        + "^http://bbs\\.163\\.com/sports/.*,"
        + "^http://t\\.163\\.com/zt/sports/.*,"
        + "^http://bbs\\.163\\.com/photoview/0T6Q0015/.*,"
        + "^http://live\\.163\\.com/room/sports/.*,"
        + "^http://zx\\.caipiao\\.163\\.com/.*,"
        + "^http://(goal|v)\\.euro2012\\.163\\.com.*,"
        + "^http://(golf|psg|sefutbol|cbf|iwf)\\.163\\.com/.*,"
        + "^http://tag\\.163\\.com/sports.*,"
        + "^http://sports\\.firefox\\.163\\.com.*,"
        + "^http://wc\\.([^\\.]+\\.)*163\\.com/.*,"
        + "^http://a\\.wc\\.163\\.com/.*,"
        + "^http://cn\\.tottenhamhotspur\\.com.*"),
    auto("auto",
        "^http://[^/]*\\.auto\\.163\\.com.*,"
        + "^http://auto[0-9]*\\.163\\.com.*,"
        + "^http://bbs\\.163\\.com/auto/.*,"
        + "^http://t\\.163\\.com/zt/auto/.*,"
        + "^http://live\\.163\\.com/room/auto/.*,"
        + "^http://tag\\.163\\.com/auto.*"),
    lady("lady",
        "^http://(lady|astro|minisite|qipai|morning|koreastyle|baby|fashion|fushi)\\.163\\.com.*,"
        + "^http://[^/]*.(lady|travel|koreastyle|baby|fushi)\\.163\\.com.*,"
        + "^http://(all|channel).163.com/lady.*,"
        + "^http://pp\\.blog\\.163\\.com.*,"
        + "^http://old.163.com/lily.*,"
        + "^http://bbs12\\.netease\\.com.*,"
        + "^http://aimer\\.163\\.com/.*,"
        + "^http://bbs\\.163\\.com/lady/.*,"
        + "^http://t\\.163\\.com/zt/lady/.*,"
        + "^http://live\\.163\\.com/room/lady/.*,"
        + "^http://data\\.art\\.163\\.com.*,"
        + "^http://miss\\.163\\.com.*,"
        + "^http://dajia\\.163\\.com.*,"
        + "^http://men\\.163\\.com.*"),
    mobile("mobile",
        "^http://[^/]*\\.mobile\\.163\\.com.*,"
        + "^http://(mobile|minisite)\\.163\\.com.*,"
        + "^http://tech\\.163\\.com/mobile/.*,"
        + "^http://product\\.tech\\.163\\.com/mobile/.*,"
        + "^http://club\\.tech\\.163\\.com/.*,"
        + "^http://vote\\.tech\\.163\\.com/guess/.*,"
        + "^http://bbs\\.163\\.com/mobile/.*,"
        + "^http://t\\.163\\.com/zt/mobile/.*,"
        + "^http://m\\.163\\.com/.*,"
        + "^http://live\\.163\\.com/room/mobile/.*,"
        + "^http://www\\.163\\.com/special/.*"),
    ent("ent",
        "^http://(ent|orz|vod|superfans|worldcup|music|pepsi|fs|xinli|philips|movie|tcl|sexy)\\.163\\.com.*,"
        + "^http://[^\\.]*\\.(ent|orz|music|pepsi|superfans|fs|tcl|sexy)\\.163\\.com.*,"
        + "^http://all\\.163\\.com/(entertainment|music).*,"
        + "^http://app\\.163.com/channel/ent.*,"
        + "^http://bbs6\\.netease\\.com.*,"
        + "^http://channel\\.163\\.com/ent.*,"
        + "^http://old\\.163\\.com/entertainment.*,"
        + "^http://minisite\\.163\\.com/2006/1110/kitty/.*,"
        + "^http://minisite.163.com/2007/0207/starcall/.*,"
        + "^http://bbs\\.163\\.com/ent/.*,"
        + "^http://t\\.163\\.com/zt/ent/.*,"
        + "^http://live\\.163\\.com/room/ent/.*,"
        + "^http://ent\\.tag\\.163\\.com.*,"
        + "^http://tag\\.163\\.com/ent.*");


    private String name;
    private List<Pattern> channelPatterns = new ArrayList<Pattern>();


    public static final String OTHER = "other";

    private static MediaUrlRegexChannel[] vaildChannelForScore = new MediaUrlRegexChannel[] {
        news, stock, tech, digi, sports, auto, lady, mobile, ent
    };



    private MediaUrlRegexChannel(String name, String channelRegexs) {
        this.name = name;
        String[] channelRegexArr = channelRegexs.split(",");
        for(String cr : channelRegexArr){
            channelPatterns.add(Pattern.compile(cr));
        }
    }


    public static String getChannelName(String url){
        for(MediaUrlRegexChannel val : MediaUrlRegexChannel.values()){
            if(val.match(url)){
                return val.getName();
            }
        }
        return OTHER;
    }


    public boolean match(String url){
        for(Pattern p : channelPatterns){
            if(p.matcher(url).matches()){
                return true;
            }
        }
        return false;
    }


    public String getName() {
        return name;
    }


}
