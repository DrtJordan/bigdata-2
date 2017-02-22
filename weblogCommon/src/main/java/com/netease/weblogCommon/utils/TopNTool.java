package com.netease.weblogCommon.utils;

import java.util.Arrays;
import java.util.Comparator;


/**
 * 利用小根堆求最大topN，当n过大时不适用
 * */
public class TopNTool {
	
	public static class SortElement{
		//根据count值排序
		private long count = 0;
		private Object val = null;
		
		public SortElement(long count, Object val) {
			this.count = count;
			this.val = val;
		}

		public long getCount() {
			return count;
		}

		public Object getVal() {
			return val;
		}
	}
	
	private final int capacity;
	private SortElement[] minHeapArr;
	private int size;
	
	public TopNTool(int n) {
		capacity = n;
		minHeapArr = new SortElement[n + 1];//数组首元素不使用
		clear();
	}
	
	public void addElement(SortElement element){
		size++;
		if(size <= capacity){
			minHeapArr[size] = element;
			siftUp();
		}else{
			minHeapArr[1] = element;
			siftDown();
		}
	}
	
	private void swap(int a, int b){
		if(a < 1 || b > capacity){
			return;
		}
		SortElement temp = minHeapArr[a];
		minHeapArr[a] = minHeapArr[b];
		minHeapArr[b] = temp;
	}
	
	//堆尾插入，向上调整堆
	private void siftUp(){
		if(size > capacity){
			return;
		}
		
		int cur = size;
		
		while(cur > 1){
			int pIndex = cur / 2;
			if(minHeapArr[pIndex].getCount() > minHeapArr[cur].getCount()){
				swap(pIndex, cur);
				cur = pIndex;
			}else{
				break;
			}
		}
	}
	
	//堆顶替换，向下调整堆
	private void siftDown(){
		if(size <= capacity){
			return;
		}
		
		int cur = 1;
		
		//while判断条件放宽，会在循环内部进一步判断，尽早退出循环
		while(cur < capacity){
			int lIndex = cur * 2;
			int rIndex = lIndex + 1;
			if(lIndex > capacity){//左节点不存在
				break;
			}
			
			int minIndex;
			if(rIndex <= capacity){//右节点存在
				if(minHeapArr[rIndex].getCount() > minHeapArr[lIndex].getCount()){
					minIndex = lIndex;
				}else{
					minIndex = rIndex;
				}
			}else{//左节点存在，右节点不存在
				minIndex = lIndex;
			}
			
			if(minHeapArr[cur].getCount() > minHeapArr[minIndex].getCount()){
				swap(cur, minIndex);
				cur = minIndex;
			}else{
				break;
			}
		}
	}
		
	
	public void clear(){
		this.size = 0;
		for(int i=0; i < minHeapArr.length; ++i){
			minHeapArr[i] = null;
		}
	}
	
	public int getSize(){
		return size;
	}
	
	public SortElement[] getTopN(){
		int len = size < minHeapArr.length ? size : minHeapArr.length - 1;
		SortElement[] res = new SortElement[len];
		for(int i = 0; i<len; ++i){
			res[i] = minHeapArr[i + 1];
		}
		Arrays.sort(res, 0, res.length, new Comparator<SortElement>() {
			@Override
			public int compare(SortElement a, SortElement b) {
				return a.getCount() > b.getCount() ? -1 : 1;
			}
		});
		
		return res;
	}
	
	public static void main(String[] args) {
		
		
		long start = System.currentTimeMillis();
		TopNTool utl = new TopNTool(100);
		for (int i = 0 ;i<200000;i++){
		    
			utl.addElement(new SortElement(i, "one"));
		}
		
		System.out.println("getInputSize = " + utl.getSize());
		
		for(SortElement ele : utl.getTopN()){
			System.out.println(ele.getCount() + " " + ele.getVal().toString());
		}
		long end = System.currentTimeMillis();
		System.out.println((end-start));
	}
}
