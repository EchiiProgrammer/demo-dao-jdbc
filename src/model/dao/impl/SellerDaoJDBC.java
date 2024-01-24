package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		PreparedStatement preprdStatement = null;
		
		try {
			preprdStatement = conn.prepareStatement(
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) " 
					+ "VALUES " 
					+ "(?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			
			preprdStatement.setString(1, obj.getName());
			preprdStatement.setString(2, obj.getEmail());
			preprdStatement.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			preprdStatement.setDouble(4,  obj.getBaseSalary());
			preprdStatement.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = preprdStatement.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = preprdStatement.getGeneratedKeys();
				if( rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! No rows affected!");
			}
			
			
			
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(preprdStatement);
		}
		
	}

	@Override
	public void update(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement prerdStatement = null;
		ResultSet resultSet = null;
		try {
			prerdStatement =  conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "		
					+ "ON seller.DepartmentId = department.Id " 
					+ "WHERE seller.Id = ?" );
			
			prerdStatement.setInt(1, id);
			resultSet = prerdStatement.executeQuery();
			if (resultSet.next()) {
				Department dep = instatianteDepartment(resultSet);
				Seller obj = instatiateSeller(resultSet, dep);
				return obj;
				
			}
			return null;
			
			
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prerdStatement);
			DB.closeResultSet(resultSet);
			
		}
		
		
	}

	private Seller instatiateSeller(ResultSet resultSet, Department dep) throws SQLException{
		Seller obj = new Seller();
		obj.setId(resultSet.getInt("Id"));
		obj.setName(resultSet.getString("Name"));
		obj.setEmail(resultSet.getString("Email"));
		obj.setBaseSalary(resultSet.getDouble("BaseSalary"));
		obj.setBirthDate(resultSet.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	private Department instatianteDepartment(ResultSet resultSet) throws SQLException{
		Department dep = new Department();
		dep.setId(resultSet.getInt("DepartmentId"));
		dep.setName(resultSet.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement prerdStatement = null;
		ResultSet resultSet = null;
		try {
			prerdStatement =  conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY Name ");
			
			resultSet = prerdStatement.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			while (resultSet.next()) {
				
				Department dep = map.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = instatianteDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller obj = instatiateSeller(resultSet, dep);
				list.add(obj);
				
			}
			return list;
			
			
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prerdStatement);
			DB.closeResultSet(resultSet);
			
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement prerdStatement = null;
		ResultSet resultSet = null;
		try {
			prerdStatement =  conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY Name ");
			
			prerdStatement.setInt(1, department.getId());
			resultSet = prerdStatement.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			while (resultSet.next()) {
				
				Department dep = map.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = instatianteDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller obj = instatiateSeller(resultSet, dep);
				list.add(obj);
				
			}
			return list;
			
			
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prerdStatement);
			DB.closeResultSet(resultSet);
			
		}
	}

}
