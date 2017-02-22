package com.netease.weblogCommon.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class BytesUtils {
	
	public static Map<String, Integer> bytesToSI(byte[] bytes) throws IOException{
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		Map<String, Integer> res = new HashMap<>();
		int size = dis.readInt();
		while(size-- > 0){
			String key = dis.readUTF();
			int val = dis.readInt();
			res.put(key, val);
		}
		
		dis.close();
		
		return res;
	}
	
	public static Map<Integer, String> bytesToIS(byte[] bytes) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        Map<Integer, String> res = new HashMap<>();
        int size = dis.readInt();
        while(size-- > 0){
            int key = dis.readInt();
            String val = dis.readUTF();
            res.put(key, val);
        }
        
        dis.close();
        
        return res;
    }
	
	public static Map<String, String> bytesToSS(byte[] bytes) throws IOException{
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		Map<String, String> res = new HashMap<>();
		int size = dis.readInt();
		while(size-- > 0){
			String key = dis.readUTF();
			String val = dis.readUTF();
			res.put(key, val);
		}
		
		dis.close();
		
		return res;
	}
	
	public static byte[] siToBytes(Map<String, Integer> map) throws IOException{
		if(map == null){
			map = new HashMap<>();
		}
		
		ByteArrayOutputStream s = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(s);
		dos.writeInt(map.size());
		for(Entry<String, Integer> entry : map.entrySet()){
			dos.writeUTF(entry.getKey());
			dos.writeInt(entry.getValue());
		}
		
		dos.close();
		s.close();
		
		return s.toByteArray();
	}
	
	public static byte[] isToBytes(Map<Integer, String> map) throws IOException{
        if(map == null){
            map = new HashMap<>();
        }
        
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(s);
        dos.writeInt(map.size());
        for(Entry<Integer, String> entry : map.entrySet()){
            dos.writeInt(entry.getKey());
            dos.writeUTF(entry.getValue());
        }
        
        dos.close();
        s.close();
        
        return s.toByteArray();
    }
	
	public static byte[] ssToBytes(Map<String, String> map) throws IOException{
		if(map == null){
			map = new HashMap<>();
		}
		
		ByteArrayOutputStream s = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(s);
		dos.writeInt(map.size());
		for(Entry<String, String> entry : map.entrySet()){
			dos.writeUTF(entry.getKey());
			dos.writeUTF(entry.getValue());
		}
		
		dos.close();
		s.close();
		
		return s.toByteArray();
	}
	
}
