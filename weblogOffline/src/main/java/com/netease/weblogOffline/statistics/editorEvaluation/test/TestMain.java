package com.netease.weblogOffline.statistics.editorEvaluation.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.netease.weblogCommon.utils.BytesUtils;

public class TestMain {

	public static void main(String[] args) throws UnsupportedEncodingException {
//		int i = (1 << 24);
//
//		System.out.println(Integer.toBinaryString(i));
//		
//		byte indicator = (byte) (i & 0xFF);
//		byte platform = (byte) (i >> 8 & 0xFF);
//		byte group = (byte) (i >> 16 & 0xFF);
//		byte pvOrUv = (byte) (i >> 24 & 0xFF);
//		System.out.println();
//		System.out.println(Integer.toBinaryString(new Integer(indicator)));
//		System.out.println(Integer.toBinaryString(new Integer(platform)));
//		System.out.println(Integer.toBinaryString(new Integer(group)));
//		System.out.println(Integer.toBinaryString(new Integer(pvOrUv)));
//		
//		System.out.println(Byte.MIN_VALUE + " " + Byte.MAX_VALUE);
		
//		byte[] bb = new byte[]{-1,-3};
//		System.out.println(bb.length);
//		for(byte a : bb){
//			System.out.print(a + " ");
//		}
//		System.out.println();
//		
//		String s = Base64.encodeBase64String(bb);
//		
//
//		byte[] bs = Base64.decodeBase64(s.getBytes("UTF-8"));
//		System.out.println(bs.length);
//		for(byte a : bs){
//			System.out.print(a + " ");
//		}
		
		
		
		try {
			Map<String, Integer> map = new HashMap<>();
			map.put("hello", 1);
			byte[] toBytes = BytesUtils.siToBytes(map);
			
			String s = new String(toBytes);
			System.out.println(toBytes.length);
			System.out.println(s.getBytes().length);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
