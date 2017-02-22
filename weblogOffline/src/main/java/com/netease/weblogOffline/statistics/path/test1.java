package com.netease.weblogOffline.statistics.path;

public class test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url = null;
		url = "http://www.baidu.com.";
		//String url1 = null;
		//url1 = getFilterUrl(filterUrl(url),".com");
		System.out.println("url = " + filterUrl(url));

	}

	private static String filterUrl(String URL){
        String filterurl = URL==null? null: URL.toLowerCase();
        if(filterurl!=null && filterurl.length()>0){
            if(filterurl.endsWith(".")){
                filterurl = filterurl.substring(0, filterurl.length()-1);
            }
            System.out.println("filterurl = " + filterurl);
            if(filterurl.contains(".cz")
                || filterurl.contains(".com")
                || filterurl.contains(".cn")
                || filterurl.contains(".org")
                || filterurl.contains(".edu")
                || filterurl.contains(".net")){

                filterurl = getFilterUrl(filterurl, ".cz");
                filterurl = getFilterUrl(filterurl, ".com");
                filterurl = getFilterUrl(filterurl, ".cn");
                filterurl = getFilterUrl(filterurl, ".org");
                filterurl = getFilterUrl(filterurl, ".edu");
                filterurl = getFilterUrl(filterurl, ".net");
                System.out.println("filterurl2 = " + filterurl);
                if(filterurl.contains(".")){
                	System.out.println("index = " + filterurl.lastIndexOf("."));
                    filterurl = filterurl.substring(filterurl.lastIndexOf(".")+1);
                    
                }
                System.out.println("filterurl3 = " + filterurl);
            }
        }
        return filterurl;
    }

    private static String getFilterUrl(String filterurl, String rule) {
        if(filterurl.endsWith(rule)){
            filterurl = filterurl.substring(0, filterurl.lastIndexOf(rule));
        }
        System.out.println("rule = " + rule);
        return filterurl;
    }
}
