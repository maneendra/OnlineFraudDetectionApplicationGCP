package fraud.bigdata.app.entity;

import java.io.Serializable;

public class MLTransaction implements Serializable{
	
	private static final long serialVersionUID = 1L;

	private double amt;
	
	private String category;
	
	private String cc_num;
	
	private double lat;
	
	private double longitude;
	
	private String trans_date_trans_time;

	public double getAmt() {
		return amt;
	}

	public void setAmt(double amt) {
		this.amt = amt;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getCc_num() {
		return cc_num;
	}

	public void setCc_num(String cc_num) {
		this.cc_num = cc_num;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String getTrans_date_trans_time() {
		return trans_date_trans_time;
	}

	public void setTrans_date_trans_time(String trans_date_trans_time) {
		this.trans_date_trans_time = trans_date_trans_time;
	}
}
