package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;

public class HCOCPlanningAlgorithm extends BasePlanningAlgorithm {

	private double deadline;
	private PCHPlanningAlgorithm pch;
	private List<CondorVM> publicVms;

	public HCOCPlanningAlgorithm(double deadline) {
		this.deadline = deadline;
		pch = new PCHPlanningAlgorithm();
		publicVms = new ArrayList<CondorVM>();
	}

	@Override
	public void run() throws Exception {
		System.out.println("\n---Begin HCOC---\n");
		pch.setTaskList(getTaskList());
		pch.setVmList(getVmList());
		pch.run();

		double makespan = pch.getMakespan();

		if (makespan <= this.deadline)
			return;

		boolean fulfilled = hcoc(makespan);
		makespan = pch.getMakespan();

		System.out.println("Makespan: " + makespan);
		System.out.println("HCOC was able to schedule the DAG:" + fulfilled);

		System.out.println("\n---END HCOC---\n");

	}

	private boolean hcoc(double makespan) {
		int iteration = 0, numberOfClusters = 0;
		List<Task> T = new ArrayList<Task>();
		List<CondorVM> R = pch.privateVms;
		List<Vm> H = new ArrayList<Vm>();

		for (Object o : getVmList()) {
			CondorVM vm = (CondorVM) o;
			if (!R.contains(vm)) {
				publicVms.add(vm);
				pch.mId2Vm.put(vm.getId(), vm);
			}
		}

		List<Task> tasks = pch.getDagSortedByPriority();

		/*
		 * line 3
		 */
		while (makespan > deadline && iteration < getTaskList().size()) {
			/*
			 * line 4
			 */
			iteration++;

			Task hp = tasks.remove(0);
			/*
			 * line 6
			 */
			H.clear();
			H.addAll(R);

			/*
			 * line 7
			 */
			T.add(hp);

			/*
			 * line 8
			 */
			numberOfClusters = getNumberOfCluster(T);

			/*
			 * line 9
			 */
			while (numberOfClusters > 0) {
				/*
				 * line 10
				 */
				Vm vm = selectResouceFromPublicCloud(numberOfClusters);

				/*
				 * line 11
				 */
				if (!H.contains(vm))
					H.add(vm);

				/*
				 * line 12
				 */
				numberOfClusters = numberOfClusters - vm.getNumberOfPes();
			}

			/*
			 * line 15
			 */
			reschedule(T, H);

			/*
			 * line 16
			 */
			for (Vm vm : H)
				pch.resetCounter(vm);

			for (Task t : pch.getDagSortedByPriority())
				pch.update(t);

			makespan = pch.getMakespan();

		}
		for (Vm vm : H)
			pch.resetCounter(vm);
		for (Task t : pch.getDagSortedByPriority())
			pch.update(t);

		System.out.println("HCOC Result:");
		for (Object o : getTaskList()) {
			Task task = (Task) o;
			Vm vm = pch.mId2Vm.get(task.getVmId());
			System.out.println(vm + " -> " + task + " : " + pch.EST.get(task)
					+ " + " + pch.getComputationCost(task, vm) + " = "
					+ pch.EFT.get(task));
		}

		return makespan <= this.deadline;
	}

	private void reschedule(List<Task> tasks, List<Vm> H) {
		/*
		 * Remove T from schedule done by PCH
		 */
		for (Task t : tasks) {

			double minEFT = Double.MAX_VALUE;
			Vm bestResource = null;
			/*
			 * Desallocate old vm
			 */
			pch.desallocate(t, pch.mId2Vm.get(t.getVmId()));

			for (Vm newVm : H) {

				pch.resetCounter(newVm);

				/*
				 * Allocate new vm
				 */
				List<Task> newCluster = pch.allocate(t, newVm);

				for (Task task : newCluster)
					pch.update(task);

				double EFT = pch.EFT.get(t);

				if (EFT < minEFT) {
					minEFT = EFT;
					bestResource = newVm;
				}

				/*
				 * Desallocate new vm
				 */
				pch.desallocate(t, pch.mId2Vm.get(t.getVmId()));
			}

			/*
			 * Allocate best vm
			 */
			pch.allocate(t, bestResource);
		}
	}

	private Vm selectResouceFromPublicCloud(int numberOfClusters) {
		Vm bestResource = null;
		double min = Double.MAX_VALUE, condition = 0.0;
		for (CondorVM vm : this.publicVms) {

			condition = vm.getPrice() / (vm.getNumberOfPes() * vm.getMips());

			if ((condition < min) && (vm.getNumberOfPes() <= numberOfClusters)) {
				min = condition;
				bestResource = vm;
			}
		}
		return bestResource;
	}

	private int getNumberOfCluster(List<Task> list) {
		HashSet<Integer> set = new HashSet<Integer>();
		for (Task t : list)
			for (int i = 0; i < this.pch.clusters.size(); i++)
				if (this.pch.clusters.get(i).contains(t))
					set.add(i);

		return set.size();
	}

}
