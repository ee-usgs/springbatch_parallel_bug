package com.example.springbatchdemo.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	//This method of dynamic construction of a parallel flow works!
	@Bean
	public Flow parallelFlowWorks() {
		SimpleAsyncTaskExecutor exe = new SimpleAsyncTaskExecutor("WorksExe");
		exe.setConcurrencyLimit(2);

		FlowBuilder<SimpleFlow> myFlow = new FlowBuilder<SimpleFlow>("WorkingParallelFlow");

		//Create a list of Steps, each single step wrapped in a flow because Flow.split.add only accepts flows
		List<Flow> flows = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			flows.add(createFlow("WorkingParallelFlow", i));
		}

		myFlow.split(exe).add(flows.toArray(new Flow[flows.size()]));   //Add flows all in one 'add' operation

		return myFlow.build();
	}

	//This method of dynamic construction of a parallel flow, results in a flow that hangs.
	@Bean
	public Flow parallelFlowNoWork() {
		SimpleAsyncTaskExecutor exe = new SimpleAsyncTaskExecutor("NoWorksExe");
		exe.setConcurrencyLimit(2);

		FlowBuilder<SimpleFlow> myFlow = new FlowBuilder<SimpleFlow>("NonWorkingParallelFlow");

		//Directly add flows to Flow.split.add, calling add for each flow (apparently doesn't work correctly)
		FlowBuilder.SplitBuilder<SimpleFlow> split = myFlow.split(exe);
		for (int i = 0; i < 10; i++) {
			split.add(createFlow("NonWorkingParallelFlow", i)); // <<< Key change - call split.add() multiple times
		}

		return myFlow.build();
	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("MyJob")
				       .start(parallelFlowWorks())
					   .next(stepBuilderFactory.get("WorkingFlowDone")
							         .tasklet((contribution, chunkContext) -> {
								         System.out.println("#### WorkingFlowDone, now starting NonWorkingFlow  ####");
								         return RepeatStatus.FINISHED;
							         }).build())
				       .next(parallelFlowNoWork())  /* This one just hangs */
				       .build().build();
	}

	//
	//Methods below just help in the construction

	/**
	 * Create a Flow two wrap a single step.
	 * Steps can't be directly added to a Flow.split.add, so this just wraps for that purpose.
	 * @param parentFlowName
	 * @param stepId
	 * @return
	 */
	public Flow createFlow(String parentFlowName, int stepId) {
		Step step = createStep(parentFlowName, stepId, stepId * 10);

		return new FlowBuilder<SimpleFlow>("WrapperFlowForStep_" + step.getName())
				       .start(step).build();
	}

	/**
	 * Create a step that just sleeps for a bit.  ParentFlowName and stepId just ensure it has a unique name.
	 * @param parentFlowName
	 * @param stepId
	 * @param taskMillsDuration
	 * @return
	 */
	public Step createStep(String parentFlowName, int stepId, int taskMillsDuration) {
		String name = parentFlowName + "_Step_" + stepId + "_duration_" + taskMillsDuration;

		return stepBuilderFactory.get(name)
		       .tasklet(new Tasklet() {
			       @Override
			       public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
				       System.out.println(">>>>>>>> Begin " + name + "      <<<<<<<<<<<<<<<<<<<<");
				       long sleepTime = sleep(taskMillsDuration);
				       System.out.println(">>>>>>>> Done " + name + "   Actual sleep ms = " + sleepTime + "      <<<<<<<<<<<<<<<<<<<<");

				       return RepeatStatus.FINISHED;
			       }
		       }).build();
	}


	/**
	 * Sleep the running thread, ignoring interruptions.
	 *
	 * @param msDuration
	 * @return Actual time slept.
	 */
	public static long sleep(int msDuration) {
		long remaining = msDuration;
		long start = System.currentTimeMillis();

		while (remaining > 0) {

			try {
				TimeUnit.MILLISECONDS.sleep(remaining);
			} catch (InterruptedException ie) {
				//ignore
			} finally {
				remaining = remaining - (System.currentTimeMillis() - start);
			}
		}

		return System.currentTimeMillis() - start;
	}

}
