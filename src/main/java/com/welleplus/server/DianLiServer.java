package com.welleplus.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.welleplus.entity.DianLiInfo;
import com.welleplus.until.MySqlBUtil;
import com.welleplus.until.SqlServerUtil;

public class DianLiServer {
	/**
	 * 读sqlserver
	 * @return
	 */
	public static List<DianLiInfo> getInfos(String time,String preTime,String prePreTime){
		List<DianLiInfo> infos = new ArrayList<DianLiInfo>();
		Connection conn = null;
		String sql = "select (ed.Value-ex.Value) as Value,ex.Time,ex.PointName from "+
					"(select e.* from exp_dianlibaobiao e, "+
					"(select min(Time) Time,PointName from exp_dianlibaobiao where Time<? and Time>=? group by PointName) x "+
					"where x.PointName=e.PointName and x.Time=e.Time) ex, "+
					" (select e.* from exp_dianlibaobiao e, "+
					"(select min(Time) Time,PointName from exp_dianlibaobiao where Time<? and Time>=? group by PointName) d "+
					" where d.PointName=e.PointName and d.Time=e.Time) ed "+
					" where ex.PointName=ed.PointName";
		Statement st = null;
		PreparedStatement ps = null;
		try {
			conn = SqlServerUtil.getConnection();
//			st = conn.createStatement();
			ps = conn.prepareStatement(sql);
			ps.setString(1, preTime);
			ps.setString(2, prePreTime);
			ps.setString(3, time);
			ps.setString(4, preTime);
			ResultSet rs = ps.executeQuery();
//			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				DianLiInfo info = new DianLiInfo();
				info.setPointName(rs.getString("PointName"));
				info.setTime(rs.getString("Time"));
				info.setValue(rs.getDouble("Value"));
				infos.add(info);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return infos;
	}
	
	/**
	 * 存入mysql原始数据
	 * @param infos
	 */
	public static void saveInfos(DianLiInfo info){
		Connection conn = null;
		PreparedStatement ps = null;
		String sql = "insert into exp_dianlibaobiao(Time,PointName,value) values(?,?,?)";
		try {
			conn = MySqlBUtil.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, info.getTime());
			ps.setString(2, info.getPointName());
			ps.setDouble(3, info.getValue());
			ps.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 存入mysql
	 * @param infos
	 */
	public static void saveActInfos(DianLiInfo info){
		Connection conn = null;
		PreparedStatement ps = null;
		String sql = "insert into exp_dianlibaobiaoact(Time,PointName,value) values(?,?,?)";
		try {
			conn = MySqlBUtil.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, info.getTime());
			ps.setString(2, info.getPointName());
			ps.setDouble(3, info.getValue());
			ps.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 获取sqlserver最老的一条数据
	 * @return
	 */
	public static DianLiInfo getInfoFromSqlServer(){
		DianLiInfo info = new DianLiInfo();
		Connection conn = null;
		Statement st = null;
		String sql = "select top 1 * from exp_dianlibaobiao order by Time";
		try {
			conn = SqlServerUtil.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				info.setId(rs.getLong("id"));
				info.setPointName(rs.getString("PointName"));
				info.setTime(rs.getString("Time"));
				info.setValue(rs.getDouble("value"));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return info;
	}
	
	/**
	 * 获取mysql中最新的一条数据
	 * @return
	 */
	public static DianLiInfo getInfoFromMySql(){
		DianLiInfo info = new DianLiInfo();
		Connection conn = null;
		Statement st = null;
		String sql = "select * from exp_dianlibaobiao order by Time desc limit 1";
		try {
			conn = MySqlBUtil.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				info.setPointName(rs.getString("PointName"));
				info.setTime(rs.getString("Time"));
				info.setValue(rs.getDouble("value"));
			}else{
				info = null;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return info;
	}
	
	/**
	 * 通过PointName和time查找数据
	 * @param pointName
	 * @param time
	 * @return
	 */
	public static List<DianLiInfo> getInfosFromMySqlUsePointNameAndTime(String pointName,String time){
		List<DianLiInfo> infos = new ArrayList<DianLiInfo>();
		Connection conn = null;
		PreparedStatement ps = null;
		String sql = "select * from exp_dianlibaobiao where PointName = ? and Time = ?";
		try {
			conn = MySqlBUtil.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, pointName);
			ps.setString(2, time);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				DianLiInfo info = new DianLiInfo();
				info.setId(rs.getLong("id"));
				info.setPointName(pointName);
				info.setTime(time);
				info.setValue(rs.getDouble("value"));
				
				infos.add(info);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(ps!=null){
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return infos;
	}
	
	
	public static List<DianLiInfo> getInfosFromMySql(){
		List<DianLiInfo> infos = new ArrayList<DianLiInfo>();
		return infos;
	}
	
	public static void main(String[] args) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		DianLiInfo info = DianLiServer.getInfoFromMySql();
		if(info==null){
			DianLiInfo oldInfo = DianLiServer.getInfoFromSqlServer();
			String oldTime = oldInfo.getTime().substring(0,oldInfo.getTime().length()-2);
			String preOldTime = sdf.format(sdf.parse(oldTime).getTime()-24*60*60*1000);
			String prePreOldTime = sdf.format(sdf.parse(preOldTime).getTime()-24*60*60*1000);
			System.out.println(oldTime);
			System.out.println(preOldTime);
			System.out.println(prePreOldTime);
			
			boolean flag = oldTime.equals(sdf.format(new Date()));
			while (!flag){
				List<DianLiInfo> infos = DianLiServer.getInfos(oldTime,preOldTime,prePreOldTime);
				for (DianLiInfo dInfo : infos) {
					System.out.println(dInfo.getPointName());
					System.out.println(dInfo.getValue());
					System.out.println(dInfo.getTime());
					List<DianLiInfo> diInfos = DianLiServer.getInfosFromMySqlUsePointNameAndTime(dInfo.getPointName(),dInfo.getTime().substring(0,dInfo.getTime().indexOf(" ")));
					if(diInfos!=null&&diInfos.size()!=0){
						continue;
					}else{
						DianLiServer.saveInfos(dInfo);
						DianLiServer.saveActInfos(dInfo);
						System.out.println("保存成功");
					}
				}
				
				oldTime = sdf.format(sdf.parse(oldTime).getTime()+24*60*60*1000);
				preOldTime = sdf.format(sdf.parse(oldTime).getTime()-24*60*60*1000);
				prePreOldTime = sdf.format(sdf.parse(preOldTime).getTime()-24*60*60*1000);
				flag = prePreOldTime.equals(sdf.format(new Date()));
			}
			
			
		}else{
			DianLiInfo diInfo = DianLiServer.getInfoFromMySql();
			String time = diInfo.getTime()+" 00:00:00";
			String nextTime = sdf.format(sdf.parse(time).getTime()+24*60*60*1000);
			String nextNextTime = sdf.format(sdf.parse(nextTime).getTime()+24*60*60*1000);
			boolean flag = time.equals(sdf.format(new Date()));
			while (!flag){
				List<DianLiInfo> infos = DianLiServer.getInfos(nextNextTime,nextTime,time);
				for (DianLiInfo dInfo : infos) {
					System.out.println(dInfo.getPointName());
					System.out.println(dInfo.getValue());
					System.out.println(dInfo.getTime());
					List<DianLiInfo> diInfos = DianLiServer.getInfosFromMySqlUsePointNameAndTime(dInfo.getPointName(),dInfo.getTime().substring(0,dInfo.getTime().indexOf(" ")));
					if(diInfos!=null&&diInfos.size()!=0){
						continue;
					}else{
						DianLiServer.saveInfos(dInfo);
						DianLiServer.saveActInfos(dInfo);
						System.out.println("保存成功");
					}
				}
				
				time = sdf.format(sdf.parse(time).getTime()+24*60*60*1000);
				nextTime = sdf.format(sdf.parse(time).getTime()+24*60*60*1000);
				nextNextTime = sdf.format(sdf.parse(nextTime).getTime()+24*60*60*1000);
				flag = time.equals(sdf.format(new Date()));
			}
		}
	}

}
