package com.welleplus.entity;

import java.io.Serializable;

public class DianLiInfo implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5289839953853580078L;
	
	private Long id;
	private String Time;
	private String PointName;
	private Double value;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTime() {
		return Time;
	}
	public void setTime(String time) {
		Time = time;
	}
	public String getPointName() {
		return PointName;
	}
	public void setPointName(String pointName) {
		PointName = pointName;
	}
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	

}
