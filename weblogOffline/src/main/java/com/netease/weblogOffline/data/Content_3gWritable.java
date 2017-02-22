/**

 */
package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.netease.weblogOffline.utils.HadoopUtils;


public class Content_3gWritable implements Writable {



    private String docid;    

    private String wUrl;
    private String gurl;
	private String editor ;//责编
    public String getEditor() {
		return editor;
	}

	public void setEditor(String editor) {
		this.editor = editor;
	}

	private ArrayList<ColumnOFContent_3gWritable> content_3g_list = new ArrayList<ColumnOFContent_3gWritable>();
    

    public Content_3gWritable() {    }

    public Content_3gWritable(Content_3gWritable one) {
    	  this.docid=one.docid;    

    	  this.wUrl=one.wUrl;
    	  this.gurl=one.gurl;
    	  this.editor=one.editor;
    	  
    	  for (ColumnOFContent_3gWritable c :one.getContent_3g_list()){
    		  content_3g_list.add(new ColumnOFContent_3gWritable(c));  
    	  }
    }

    /**
     * 
     * 
     */
    public Content_3gWritable(String docid,String wUrl,String gurl,String editor,ArrayList<ColumnOFContent_3gWritable> content_3g_list) {
        
       for (ColumnOFContent_3gWritable c :content_3g_list){
    	   this.content_3g_list.add(new ColumnOFContent_3gWritable(c));  
 	   }

	  this.docid=docid;    
	  this.editor=editor;
	  this.wUrl=wUrl;
	  this.gurl=gurl;
    }



	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}


	public String getwUrl() {
		return wUrl;
	}

	public void setwUrl(String wUrl) {
		this.wUrl = wUrl;
	}

	public String getGurl() {
		return gurl;
	}

	public void setGurl(String gurl) {
		this.gurl = gurl;
	}

	public void readFields(DataInput in) throws IOException {
	    content_3g_list.clear();
		docid = in.readUTF();
		wUrl = in.readUTF();
		gurl = in.readUTF();
		editor = in.readUTF();
		int size = in.readInt();
		for (int i = 0;i<size;i++){
			ColumnOFContent_3gWritable  fcoc =new ColumnOFContent_3gWritable();
			fcoc.readFields(in);
			content_3g_list.add(fcoc);
		}
      }

    public void write(DataOutput out) throws IOException {

    	HadoopUtils.writeString(out, docid);
       	HadoopUtils.writeString(out, wUrl);
    	HadoopUtils.writeString(out, gurl);
    	HadoopUtils.writeString(out, editor);
    	out.writeInt(content_3g_list.size());
    	for (ColumnOFContent_3gWritable fcoc :content_3g_list){
    		fcoc.write(out);
    	}
    }

  

    public ArrayList<ColumnOFContent_3gWritable> getContent_3g_list() {
		return content_3g_list;
	}

	public void setContent_3g_list(ArrayList<ColumnOFContent_3gWritable> content_3g_list) {
		this.content_3g_list.clear();
	
		for (ColumnOFContent_3gWritable cc :content_3g_list){
			this.content_3g_list.add(new ColumnOFContent_3gWritable(cc) );
		}
	}

    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(docid).append("\t").append(wUrl).append("\t").append(gurl).append("\t").append(editor).append("\t").append(content_3g_list.toString());
        return  sb.toString();
    }

}
