package com.mohanty.app;

import java.io.File;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BatchPocApplication {

	public static void main(String[] args) {
		System.setProperty("input", "file:\\" + new File(
				"C:\\Users\\002L2N744\\Documents\\RTEST-applications-docs\\production-code\\proof-of-concepts\\batch-poc\\src\\main\\resources\\in.csv")
						.getAbsolutePath());
		System.setProperty("output", "file:\\" + new File(
				"C:\\Users\\002L2N744\\Documents\\RTEST-applications-docs\\production-code\\proof-of-concepts\\batch-poc\\src\\main\\resources\\out.csv")
						.getAbsolutePath());
		SpringApplication.run(BatchPocApplication.class, args);
	}

}
