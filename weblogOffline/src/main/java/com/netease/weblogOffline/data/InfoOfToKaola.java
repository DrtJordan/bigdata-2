package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class InfoOfToKaola  implements Writable {

	
	HashMap<String,Integer> hm = new HashMap<String,Integer>();
//	private int  count = 0;
//	private int  countUV = 0;
//	private int countEmail=0;
//	private int countRD =0;
//	private int countHome =0;
//	private int auto  =0;
//	private int baby  =0;
//	private int digi  =0;
//	private int edu  =0;
//	private int ent  =0;
//	private int jiankang  =0;
//	private int lady  =0;
//	private int men   =0;
//	private int mobile  =0;
//	private int money  =0;
//	private int news   =0;
//	private int other  =0;
//	private int sport   =0;
//	private int tech   =0;
//	private int travel  =0;	
//	private int nav =0;


	public InfoOfToKaola(){
		
	}



	public InfoOfToKaola(int count, int countUV, int countEmail, int countRD,
			int countHome, HashMap<String, Integer> hm, String channel) {

		this.hm = hm;
	}



	public HashMap<String, Integer> getHm() {
		return hm;
	}



	public void setHm(HashMap<String, Integer> hm) {
		this.hm = hm;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	  out.writeInt(this.hm.size());
	  
	  for (String s :hm.keySet()){
		  out.writeUTF(s);
		  out.writeInt(hm.get(s));
	   }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		HashMap<String,Integer> hm2 = new HashMap<String,Integer>();
		int size =in.readInt();
		for (int i=0;i<size;i++){
			String key = in.readUTF();
			Integer value = in.readInt();
			hm2.put(key, value);
		}
		this.hm = hm2;
	}

}
