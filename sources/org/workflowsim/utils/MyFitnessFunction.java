package org.workflowsim.utils;

import java.util.List;
import java.util.Map;

import net.sourceforge.jswarm_pso.FitnessFunction;

import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;

public class MyFitnessFunction extends FitnessFunction {

	private Map<Task, Map<CondorVM, Double>> TP;
	private Map<CondorVM, Map<CondorVM, Double>> PP;
	private Map<Task, Map<Task, Double>> e;
	private List<Task> currentTasks;
	private List<Task> allTasks;
	private List<Vm> vmList;

	public MyFitnessFunction(boolean maximize,
			Map<Task, Map<CondorVM, Double>> TP,
			Map<CondorVM, Map<CondorVM, Double>> PP,
			Map<Task, Map<Task, Double>> e, List<Vm> vmList,
			List<Task> readyTasks, List<Task> allTasks) {
		super(maximize);
		this.TP = TP;
		this.PP = PP;
		this.e = e;
		this.vmList = vmList;
		this.currentTasks = readyTasks;
		this.allTasks = allTasks;
	}

	/**
	 * Evaluates a particles at a given position
	 */
	@Override
	public double evaluate(double[] position) {
		return getCostMaximization(position);
	}

	/**
	 * Equation 4 of the paper
	 */
	private double getCostMaximization(double[] position) {
		double maxCost = Double.MIN_VALUE;

		for (Object vmObject : vmList) {
			CondorVM vm = (CondorVM) vmObject;
			double currentCost = getTotalCost(position, vm);
			if (currentCost > maxCost)
				maxCost = currentCost;
		}
		return maxCost;
	}

	/**
	 * Equation 3 of the paper
	 */
	private double getTotalCost(double[] position, CondorVM vm) {
		return (getTrasmissionCost(position, vm) + getComputationCost(position,
				vm));
	}

	/**
	 * Equation 2 of the paper
	 */
	private double getTrasmissionCost(double[] position, CondorVM currentVm) {
		double totalTransferCost = 0.0;

		for (Object taskObject : allTasks) {
			Task task = (Task) taskObject;
			int vm1Id = task.getVmId();

			if (vm1Id < 0)
				vm1Id = (int) Math.round(position[this.currentTasks
						.indexOf(task)]);

			if (vm1Id == currentVm.getId()) {

				for (Object taskObject2 : allTasks) {
					Task task2 = (Task) taskObject2;
					int vm2Id = task2.getVmId();

					if (vm2Id < 0)
						vm2Id = (int) Math.round(position[this.currentTasks
								.indexOf(task2)]);

					if (vm2Id != currentVm.getId()
							&& task2.getParentList().contains(task))
						totalTransferCost += PP.get(currentVm).get(
								vmList.get(vm2Id))
								* e.get(task).get(task2);
				}

			}

		}

		return totalTransferCost;
	}

	/**
	 * Equation 1 of the paper
	 */
	private double getComputationCost(double[] position, CondorVM vm) {
		double totalComputationCost = 0.0;

		for (int dimension = 0; dimension < position.length; dimension++)
			if (vm.getId() == (int) Math.round(position[dimension]))
				totalComputationCost += TP.get(currentTasks.get(dimension))
						.get(vmList.get((int) Math.round(position[dimension])));
		return totalComputationCost;
	}

}