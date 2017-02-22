package com.netease.weblogOffline.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.netease.weblogOffline.utils.HadoopUtils;

/**
 * Created by hfchen on 2015/3/26.
 */
public class MediaScoreVector extends ContentScoreVector{

    // 所有有效数据项个数，不包括该项本身
    private int keySize = 16;



    private int allURLCount;

    private String subMedia; //子媒
    private String company; // 公司

    public MediaScoreVector(){}
    
    public static void main(String[] args) {
    	MediaScoreVector m1 = new MediaScoreVector();
    	m1.setUrl("url");
    	m1.setPv(1);
    	MediaScoreVector m2 = new MediaScoreVector();
    	m2.setUrl("url");
    	m2.setPv(2);
    	MediaScoreVector m3 = new MediaScoreVector();
    	m3.setUrl("url");
    	boolean add1 = m3.add(m1);
    	boolean add2 = m3.add(m2);
    	System.out.println(add1 + "," + add2);
    	System.out.println(m3);
	}


    public MediaScoreVector(ContentScoreVector csw) {
        setUrl(csw.getUrl());
        setChannel(csw.getChannel());
        setSource(csw.getSource());
        setType(csw.getType());
        setLmodify(csw.getLmodify());
        setTitle(csw.getTitle());
        setAuthor(csw.getAuthor());
        setPv(csw.getPv());
        setUv(csw.getUv());
        setGenTieCount(csw.getGenTieCount());
        setGenTieUv(csw.getGenTieUv());
        setShareCount(csw.getShareCount());
        setBackCount(csw.getBackCount());
    }


    @Override
    public String toString() {
        return "url=" + getUrl() + ",channel=" + getChannel() + ",source=" + getSource()
            + ",type=" + getType() + ",lmodify=" + getLmodify() + ",title=" + getTitle()
            + ",author=" + getAuthor() + ",pv=" + getPv() + ",uv=" + getUv()
            + ",genTieCount=" + getGenTieCount() + ",genTieUv=" + getGenTieUv()
            + ",shareCount=" + getShareCount() + ",backCount=" + getBackCount()
            + ",subMedia=" + subMedia + ",company=" + company + ",allURLCount=" + allURLCount;
    }
    
	public boolean add(ContentScoreVector csv) {
		if (!this.getUrl().equals(csv.getUrl())) {
			return false;
		}
		this.setPv(this.getPv() + csv.getPv());
		this.setUv(this.getUv() + csv.getUv());
		this.setGenTieCount(this.getGenTieCount() + csv.getGenTieCount());
		this.setGenTieUv(this.getGenTieUv() + csv.getGenTieUv());
		this.setShareCount(this.getShareCount() + csv.getShareCount());
		this.setBackCount(this.getBackCount() + csv.getBackCount());
		return true;
	}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.keySize);
        HadoopUtils.writeString(out, this.getSubMedia());
        HadoopUtils.writeString(out, this.getUrl());
        HadoopUtils.writeString(out, this.getChannel());
        HadoopUtils.writeString(out, this.getSource());
        HadoopUtils.writeString(out, this.getType());
        HadoopUtils.writeString(out, this.getLmodify());
        HadoopUtils.writeString(out, this.getTitle());
        HadoopUtils.writeString(out, this.getAuthor());
        out.writeInt(this.getPv());
        out.writeInt(this.getUv());
        out.writeInt(this.getGenTieCount());
        out.writeInt(this.getGenTieUv());
        out.writeInt(this.getShareCount());
        out.writeInt(this.getBackCount());
        out.writeInt(this.getAllURLCount());
        HadoopUtils.writeString(out, this.getCompany());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.keySize = in.readInt();
        int count = this.keySize;
        if (count-- > 0) {
            setSubMedia(in.readUTF());
        }
        if (count-- > 0) {
            setUrl(in.readUTF());
        }
        if (count-- > 0) {
            setChannel(in.readUTF());
        }
        if (count-- > 0) {
            setSource(in.readUTF());
        }
        if (count-- > 0) {
            setType(in.readUTF());
        }
        if (count-- > 0) {
            setLmodify(in.readUTF());
        }
        if (count-- > 0) {
            setTitle(in.readUTF());
        }
        if (count-- > 0) {
            setAuthor(in.readUTF());
        }
        if (count-- > 0) {
            setPv(in.readInt());
        }
        if (count-- > 0) {
            setUv(in.readInt());
        }
        if (count-- > 0) {
            setGenTieCount(in.readInt());
        }
        if (count-- > 0) {
            setGenTieUv(in.readInt());
        }
        if (count-- > 0) {
            setShareCount(in.readInt());
        }
        if (count-- > 0) {
            setBackCount(in.readInt());
        }
        if (count-- > 0) {
            setAllURLCount(in.readInt());
        }
        if (count-- > 0) {
            setCompany(in.readUTF());
        }

    }
    
    @Override
    public MediaScoreVector clone() {
        MediaScoreVector mediaScoreVector = new MediaScoreVector();
        mediaScoreVector.setAllURLCount(this.getAllURLCount());
        mediaScoreVector.setCompany(this.getCompany());
        mediaScoreVector.setSubMedia(this.getSubMedia());
        mediaScoreVector.setAuthor(this.getAuthor());
        mediaScoreVector.setBackCount(this.getBackCount());
        mediaScoreVector.setChannel(this.getChannel());
        mediaScoreVector.setGenTieCount(this.getGenTieCount());
        mediaScoreVector.setGenTieUv(this.getGenTieUv());
        mediaScoreVector.setLmodify(this.getLmodify());
        mediaScoreVector.setPv(this.getPv());
        mediaScoreVector.setShareCount(this.getShareCount());
        mediaScoreVector.setSource(this.getSource());
        mediaScoreVector.setTitle(this.getTitle());
        mediaScoreVector.setType(this.getType());
        mediaScoreVector.setUrl(this.getUrl());
        mediaScoreVector.setUv(this.getUv());
        return mediaScoreVector;
    }

    public String getSubMedia() {
        return subMedia;
    }

    public void setSubMedia(String subMedia) {
        this.subMedia = subMedia;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public void setAllURLCount(int allURLCount) {
        this.allURLCount = allURLCount;
    }

    public int getAllURLCount() {
        return allURLCount;
    }
}
