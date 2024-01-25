package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {

	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	
	@Override
	public void insert(Department obj) {
		PreparedStatement preprdStatement = null;

		try {
			preprdStatement = conn.prepareStatement(
					"INSERT INTO department "
							+ "(Name) " 
							+ "VALUES " 
							+ "(?)",
							Statement.RETURN_GENERATED_KEYS);

			preprdStatement.setString(1, obj.getName());

			int rowsAffected = preprdStatement.executeUpdate();

			if(rowsAffected > 0) {
				ResultSet rs =preprdStatement.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				else {
					throw new DbException("Unexpected error no rows affected");
				}
			}

		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(preprdStatement);
		}
	}
			
		

	@Override
	public void update(Department obj) {


		PreparedStatement preprdStatement = null;

		try {
			preprdStatement = conn.prepareStatement(
					"UPDATE department " 
							+ "SET Name = ? " 
							+ "WHERE Id = ? ");

			preprdStatement.setString(1, obj.getName());
			preprdStatement.setInt(2, obj.getId());
			
			preprdStatement.executeUpdate();


		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
	}

	
	@Override
	public void deleteById(Integer id) {
		PreparedStatement auxSt = null;
		try {
			auxSt = conn.prepareStatement(
					"DELETE FROM department WHERE id = ?");
			
			auxSt.setInt(1, id);
			
			auxSt.executeUpdate();
		}
		catch(SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		}
		finally {
			DB.closeStatement(auxSt);
		}
		
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement prerdStatement = null;
		ResultSet resultSet = null;
		try {
			prerdStatement =  conn.prepareStatement(
					"SELECT * FROM department WHERE Id = ?" );
			
			prerdStatement.setInt(1, id);
			resultSet = prerdStatement.executeQuery();
			if (resultSet.next()) {
				Department obj = new Department();
				obj.setId(resultSet.getInt("Id"));
				obj.setName(resultSet.getString("Name"));
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

	@Override
	public List<Department> findAll() {
		PreparedStatement prerdStatement = null;
		ResultSet resultSet = null;
		try {
			prerdStatement = conn.prepareStatement(
					"SELECT * FROM department");

			resultSet = prerdStatement.executeQuery();


			List<Department> list = new ArrayList<>();
			
			while(resultSet.next()) {
				Department dep = new Department();
				dep.setId(resultSet.getInt("Id"));
				dep.setName(resultSet.getString("Name"));
				list.add(dep);

			}
			return list;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		
		finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(prerdStatement);
		}
		
	}

}
