package com.mohanty.app.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.mohanty.app.pojo.Employee;

import lombok.NoArgsConstructor;

@Configuration
@EnableBatchProcessing
@NoArgsConstructor
public class BatchConfiguration {

	@Bean
	public Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, ItemReader<? extends Employee> ir,
			ItemWriter<? super Employee> iw) {
		
		Step step1 = sbf.get("file-to-db")
		.<Employee, Employee> chunk(50)
		.reader(ir)
		.writer(iw)
		.build();
		
		Job job = jbf.get("etl")
		.incrementer(new RunIdIncrementer())
		.start(step1)
		.build();
		
		return job;
	}
	
	@Bean
	FlatFileItemReader<Employee> flatFileItemReader(@Value("${input}") Resource in, FieldSetMapper<Employee> fieldSetMapper) {
		
		return new FlatFileItemReaderBuilder<Employee>()
				.name("file-reader")
				.resource(in)
				.delimited().delimiter(",").names(new String[] {"EmpId","EmpName","EmpSalary","EmpDesignation"})
				.targetType(Employee.class)
				.fieldSetMapper(fieldSetMapper)
				.linesToSkip(1)
				.build();				
	}
	
	@Bean
	JdbcBatchItemWriter<Employee> jdbcBatchItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Employee>()
				.dataSource(dataSource)
				.sql("insert into EMPLOYEE (EMP_ID, EMP_NAME, EMP_SALARY, EMP_DESIGNATION)"
						+ "values(:empId, :empName, :empSalary, :empDesignation)")
				.beanMapped()
				.build();
		
	}
	
	@Bean
	FieldSetMapper<Employee> fieldSetMapper() {
		return new EmployeeFieldSetMapper();
	}
	
}
