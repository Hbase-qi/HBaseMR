package com.qi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class User implements WritableComparable<User>,DBWritable{

	private Integer id;
	
	private String name;
	
	private Integer age;

	public User() {
		super();
	}

	public User(Integer id, String name, Integer age) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + ", age=" + age + "]";
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		id=in.readInt();
		name=in.readUTF();
		age=in.readInt();
	}

	@Override
	public int compareTo(User o) {
		
		return this.id.compareTo(o.getId());
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
		statement.setInt(1, id);
		statement.setString(2, name);
		statement.setInt(3, age);
		
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {

		id=resultSet.getInt(1);
		name=resultSet.getString(2);
		age=resultSet.getInt(3);
	}
	
	
}
