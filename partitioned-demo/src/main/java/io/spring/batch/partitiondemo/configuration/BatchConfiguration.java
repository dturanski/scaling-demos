/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.partitiondemo.configuration;

import java.net.MalformedURLException;
import java.util.*;
import javax.sql.DataSource;

import io.spring.batch.partitiondemo.domain.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.batch.partition.EnvironmentVariablesProvider;
import org.springframework.cloud.task.batch.partition.PassThroughCommandLineArgsProvider;
import org.springframework.cloud.task.batch.partition.SimpleEnvironmentVariablesProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.UrlResource;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Michael Minella
 */
@Configuration
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private ConfigurableApplicationContext context;

	@Bean
	public DeployerPartitionHandler partitionHandler(TaskLauncher taskLauncher,
			JobExplorer jobExplorer,
			Environment environment) throws MalformedURLException {

		//Need to do this to get the CF deployer to work
		Resource resource = new UrlResource("http://nothing");

		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "step1");

		List<String> commandLineArgs = new ArrayList<>(3);
		commandLineArgs.add("--spring.profiles.active=worker");
		//commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
		//commandLineArgs.add("--spring.batch.initializer.enabled=false");
		//commandLineArgs.add("--spring.datasource.initialize=false");

		partitionHandler.setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
		partitionHandler.setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(environment));
		partitionHandler.setMaxWorkers(3);
		partitionHandler.setApplicationName("PartitionedBatchJobTask");
		Map<String,String> deploymentProperties = new HashMap<>();
		deploymentProperties.put("spring.cloud.deployer.cloudfoundry.services","mysql");
		deploymentProperties.put("spring.cloud.deployer.cloudfoundry.push-task-apps-enabled","false");
		partitionHandler.setDeploymentProperties(deploymentProperties);

		return partitionHandler;
	}

	@Bean
	@StepScope
	public MultiResourcePartitioner partitioner(@Value("#{jobParameters['inputFiles']}") Resource[] resources) {
		MultiResourcePartitioner partitioner = new MultiResourcePartitioner();

		partitioner.setKeyName("file");
		partitioner.setResources(resources);

		return partitioner;
	}

	@Bean
	@Profile("worker")
	public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
		return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Transaction> fileTransactionReader(
			@Value("#{stepExecutionContext['file']}") Resource resource) {

		return new FlatFileItemReaderBuilder<Transaction>()
				.name("flatFileTransactionReader")
				.resource(resource)
				.delimited()
				.names(new String[] {"account", "amount", "timestamp"})
				.fieldSetMapper(fieldSet -> {
					Transaction transaction = new Transaction();

					transaction.setAccount(fieldSet.readString("account"));
					transaction.setAmount(fieldSet.readBigDecimal("amount"));
					transaction.setTimestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"));

					return transaction;
				})
				.build();
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TRANSACTION (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
				.build();
	}

	@Bean
	public Step partitionedMaster(PartitionHandler partitionHandler) {
		return this.stepBuilderFactory.get("step1")
				.partitioner(step1().getName(), partitioner(null))
				.step(step1())
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	public Step step1() {
		return this.stepBuilderFactory.get("step1")
				.<Transaction, Transaction>chunk(100)
				.reader(fileTransactionReader(null))
				.writer(writer(null))
				.build();
	}

	@Bean
	@StepScope
	public MultiResourceItemReader<Transaction> multiResourceItemReader(
			@Value("#{jobParameters['inputFiles']}") Resource[] resources) {

		return new MultiResourceItemReaderBuilder<Transaction>()
				.delegate(delegate())
				.name("multiresourceReader")
				.resources(resources)
				.build();
	}

	@Bean
	public DelegatingResourceLoader delegatingResourceLoader(){
		return new DelegatingResourceLoader();
	}

	@Bean
	public FlatFileItemReader<Transaction> delegate() {
		return new FlatFileItemReaderBuilder<Transaction>()
				.name("flatFileTransactionReader")
				.delimited()
				.names(new String[] {"account", "amount", "timestamp"})
				.fieldSetMapper(fieldSet -> {
					Transaction transaction = new Transaction();

					transaction.setAccount(fieldSet.readString("account"));
					transaction.setAmount(fieldSet.readBigDecimal("amount"));
					transaction.setTimestamp(fieldSet.readDate("timestamp", "yyyy-MM-dd HH:mm:ss"));

					return transaction;
				})
				.build();
	}

	@Bean
	@Primary
	public JobLauncher myJobLauncher(JobRepository jobRepository) throws Exception {
			MyJobLauncher jobLauncher = new MyJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.afterPropertiesSet();
			return jobLauncher;
	}


	@Bean
	@Profile("!worker")
	public Job parallelStepsJob() {
		return this.jobBuilderFactory.get("parallelStepsJob")
				.start(partitionedMaster(null))
				.build();
	}

	public static class MyJobLauncher extends SimpleJobLauncher {

		private static Logger logger = LoggerFactory.getLogger(SimpleJobLauncher.class);

		@Override
		public JobExecution run(final Job job, final JobParameters jobParameters)
				throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException,
				JobParametersInvalidException {
			logger.info("enriching parameters");
			JobParameters encrichedJobParameters =
					new JobParametersBuilder()
							.addJobParameters(jobParameters)
							.addString("id",UUID.randomUUID().toString())
							.toJobParameters();

			return super.run(job, encrichedJobParameters);
		}
	}

}
