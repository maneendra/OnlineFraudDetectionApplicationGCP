package fraud.bigdata.app.entity;

import java.io.Serializable;
import java.util.Objects;

public class TransactionStatus implements Serializable{
	
	private static final long serialVersionUID = 1L;

	private double amount;
	
	private String category;
	
	private double longitude;
	
	private double latitude;
	
	private String cc_number;
	
	private String trans_date_and_time;
	
	private int ml_fraud_status;
	
	private int rule_fraud_status;

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public String getCc_number() {
		return cc_number;
	}

	public void setCc_number(String cc_number) {
		this.cc_number = cc_number;
	}

	public String getTrans_date_and_time() {
		return trans_date_and_time;
	}

	public void setTrans_date_and_time(String trans_date_and_time) {
		this.trans_date_and_time = trans_date_and_time;
	}

	public int getMl_fraud_status() {
		return ml_fraud_status;
	}

	public void setMl_fraud_status(int ml_fraud_status) {
		this.ml_fraud_status = ml_fraud_status;
	}

	public int getRule_fraud_status() {
		return rule_fraud_status;
	}

	public void setRule_fraud_status(int rule_fraud_status) {
		this.rule_fraud_status = rule_fraud_status;
	}

	@Override
	public int hashCode() {
		return Objects.hash(amount, category, cc_number, ml_fraud_status, rule_fraud_status);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionStatus other = (TransactionStatus) obj;
		return Double.doubleToLongBits(amount) == Double.doubleToLongBits(other.amount)
				&& Objects.equals(category, other.category) && Objects.equals(cc_number, other.cc_number)
				&& ml_fraud_status == other.ml_fraud_status && rule_fraud_status == other.rule_fraud_status;
	}
}
