package com.mohanty.app.config;

import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
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
	public Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, Step1Configuration step1Config,
			Step2Configuration step2config) {
		
		Step step1 = sbf.get("step1: file-to-db")
		.<Employee, Employee> chunk(50)
		.reader(step1Config.flatFileItemReader(null, null))
		.writer(step1Config.jdbcBatchItemWriter(null))
		.build();
		
		Step step2 = sbf.get("step2: db-to-file")
				.<Map<String,Integer>,Map<String,Integer>>chunk(100)
				.reader(step2config.jdbcCursorItemReader(null))
				.writer(step2config.fileItemWriter(null))
				.build();
		
		Job job = jbf.get("etl job")
		.incrementer(new RunIdIncrementer())
		.start(step1)
		.next(step2)
		.build();
		
		return job;
	}
	
	/**
	 * This step is to read the data from CSV file and store it to Database 
	 * @author 002L2N744
	 *
	 */
	@Configuration
	public static class Step1Configuration {
		
		@Bean
		ItemReader<Employee> flatFileItemReader(@Value("${input}") Resource in,
				FieldSetMapper<Employee> fieldSetMapper) {
			
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
		ItemWriter<Employee> jdbcBatchItemWriter(DataSource dataSource) {
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
	
	/**
	 * This step will read the data from Database, Process the data, Write the data to a CSV file 
	 * @author 002L2N744
	 *
	 */
	@Configuration
	public static class Step2Configuration { 
		
		@Bean
		ItemReader<Map<String,Integer>> jdbcCursorItemReader(DataSource dataSource) {
			return new JdbcCursorItemReaderBuilder<Map<String,Integer>>()
					.name("database-reader")
					.dataSource(dataSource)
					.sql("select emp_designation as designation , count(*) as count_people\r\n"
							+ "from EMPLOYEE\r\n"
							+ "group by emp_designation\r\n"
							+ "order by 1")
					.rowMapper( (resultSet, rowNum) -> 
							Collections.singletonMap(resultSet.getString("designation")
													,resultSet.getInt("count_people")))
					.build();
		}
		
		@Bean
		ItemWriter<Map<String,Integer>> fileItemWriter(@Value("${output}") Resource out) {
			return new FlatFileItemWriterBuilder<Map<String,Integer>>()
					.name("file-writer")
					.resource(out)
					.lineAggregator(new DelimitedLineAggregator<Map<String,Integer>>() {
						{
							setDelimiter(",");
							setFieldExtractor( fieldExtractorMap -> {
							 Map.Entry<String, Integer> next = fieldExtractorMap.entrySet().iterator().next();
							 return new Object[] {next.getKey(), next.getValue()};
							} );
						}
						
					})
					.build();
		}
	}
	
}
