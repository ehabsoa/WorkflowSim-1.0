package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sourceforge.jswarm_pso.Swarm;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.MyFitnessFunction;
import org.workflowsim.utils.MyParticle;
import org.workflowsim.utils.Parameters;

public class PSOPlanningAlgorithm extends BasePlanningAlgorithm {

	private static final int MAX_ITERATION = 50;

	public static int DIMENSION = 0;

	private Map<Task, Map<CondorVM, Double>> TP;

	private Map<CondorVM, Map<CondorVM, Double>> PP;

	private Map<Task, Map<Task, Double>> e;

	private List<Task> mapping;

	public PSOPlanningAlgorithm() {
		TP = new HashMap<Task, Map<CondorVM, Double>>();
		PP = new HashMap<CondorVM, Map<CondorVM, Double>>();
		e = new HashMap<Task, Map<Task, Double>>();
	}

	@Override
	public void run() throws Exception {
		schedulingHeuristic();
	}

	private void schedulingHeuristic() {
		/*
		 * Step 1: Calculate average computation cost of all tasks in all
		 * compute resources
		 */
		averageComputationCostAllTaskInAllResources();

		/*
		 * Step 2: Calculate average cost of communication between resources
		 */
		averageCommunicationCostBetweenResources();

		/*
		 * Step 3: Set task node weight w(k,j) as average computation cost. In
		 * fact, w(k,j) == TP[k][j], where k is a task and j is a resource
		 */

		/*
		 * Step 4: Set edge weight e(k1,k2) as size of all file transferred
		 * between tasks k1 and k2
		 */
		calculateEdgeWeight();

		/*
		 * Step 5: Compute PSO({ a set of all tasks }). Initialize the
		 * readyTasks vector with the list of all tasks
		 */

		/*
		 * Call PSO Algorithm
		 */
		double bestPosition[] = pso(getTaskList());
		for (int d = 0; d < bestPosition.length; d++) {
			System.out.println("Task[" + d + "] => VM["
					+ Math.round(bestPosition[d]) + "]");
			Task t = (Task) getTaskList().get(d);
			t.setVmId(((int) Math.round(bestPosition[d])));
		}

		mapping = new ArrayList<Task>(getTaskList());

	}

	public void doOnline(List<Cloudlet> readyTask) {
		double bestPosition[] = pso(mapping);
		System.out.println("------");

		for (Cloudlet cloudlet : readyTask) {
			Job job = (Job) cloudlet;

			if (!job.getTaskList().isEmpty()) {
				Task t = job.getTaskList().get(0);

				for (int d = 0; d < bestPosition.length; d++) {
					if (t.equals((Task) mapping.get(d))) {
						System.out.println("Task[" + d + "] => VM["
								+ Math.round(bestPosition[d]) + "]");
						t.setVmId(((int) Math.round(bestPosition[d])));
						job.setVmId(((int) Math.round(bestPosition[d])));
					}
				}
			}

		}

	}

	private void averageComputationCostAllTaskInAllResources() {

		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			Map<CondorVM, Double> costsVm = new HashMap<CondorVM, Double>();

			for (Object vmObject : getVmList()) {
				CondorVM vm = (CondorVM) vmObject;
				if (vm.getNumberOfPes() < task.getNumberOfPes())
					costsVm.put(vm, Double.MAX_VALUE);
				else
					costsVm.put(
							vm,
							(task.getCloudletTotalLength() / vm.getMips())
									* vm.getPrice());
			}

			this.TP.put(task, costsVm);
		}
	}

	private void averageCommunicationCostBetweenResources() {

		for (Object vmObject : getVmList()) {
			CondorVM vm1 = (CondorVM) vmObject;
			Map<CondorVM, Double> costs = new HashMap<CondorVM, Double>();
			for (Object vmObject2 : getVmList()) {
				CondorVM vm2 = (CondorVM) vmObject2;
				if (vm1.equals(vm2))
					costs.put(vm2, 0.0);
				else
					costs.put(
							vm2,
							vm2.getDataTransferOutPrice()
									/ getBandwith(vm1, vm2));
			}
			this.PP.put(vm1, costs);
		}

	}

	private void calculateEdgeWeight() {
		for (Object parentObject : getTaskList()) {
			Task parent = (Task) parentObject;
			Map<Task, Double> children = new HashMap<Task, Double>();

			for (Task child : parent.getChildList()) {
				children.put(child,
						calculateSizeOfAllFilesTransfer(parent, child));
			}
			e.put(parent, children);
		}

	}

	/**
	 * Accounts the MB to transfer all files described between parent and child
	 * 
	 * @param parent
	 * @param child
	 * @return size of all files
	 */
	@SuppressWarnings("unchecked")
	private double calculateSizeOfAllFilesTransfer(Task parent, Task child) {
		List<File> parentFiles = (List<File>) parent.getFileList();
		List<File> childFiles = (List<File>) child.getFileList();

		double sum = 0.0;

		for (File parentFile : parentFiles) {
			if (parentFile.getType() != Parameters.FileType.OUTPUT.value) {
				continue;
			}

			for (File childFile : childFiles) {
				if (childFile.getType() == Parameters.FileType.INPUT.value
						&& childFile.getName().equals(parentFile.getName())) {
					sum += childFile.getSize();
					break;
				}
			}
		}

		return sum;
	}

	/**
	 * The PSO_Algorithm function: it receives a list with all tasks
	 * 
	 * @param readyTasks
	 * 
	 * @param dag
	 */
	private double[] pso(List<Task> readyTasks) {

		/*
		 * Step 1: Set particle dimension as equal to the size of ready tasks
		 * list
		 */
		DIMENSION = readyTasks.size();

		if (DIMENSION == 0)
			return null;

		/*
		 * Step 2: Initialize particles position and velocity randomly
		 */
		Swarm swarm = new Swarm(
				getTaskList().size() < Swarm.DEFAULT_NUMBER_OF_PARTICLES ? Swarm.DEFAULT_NUMBER_OF_PARTICLES
						: getTaskList().size(), new MyParticle(),
				new MyFitnessFunction(false, this.TP, this.PP, this.e,
						getVmList(), readyTasks, getTaskList()));

		swarm.setMaxPosition(getVmList().size() - 1);
		swarm.setMinPosition(0);

		for (int i = 0; i < MAX_ITERATION; i++)
			swarm.evolve();

		return swarm.getBestPosition();
	}

	private double getBandwith(CondorVM j, CondorVM k) {
		// transfers between two VMs is limited to both VMs
		return Math.min(j.getBw(), k.getBw());
	}

}
