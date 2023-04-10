package com.mohanty.app.config;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import com.mohanty.app.pojo.Employee;

public class EmployeeFieldSetMapper implements FieldSetMapper<Employee> {

	@Override
	public Employee mapFieldSet(FieldSet fieldSet) throws BindException {
		// TODO Auto-generated method stub
		return new Employee( 
				fieldSet.readInt("empId"),
				fieldSet.readString("empName"),
				fieldSet.readInt("empSalary"),
				fieldSet.readString("empDesignation") );
	}

}
