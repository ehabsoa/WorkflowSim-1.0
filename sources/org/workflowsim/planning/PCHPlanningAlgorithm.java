package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Stack;
import java.util.Vector;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.PlanningAlgorithm;

public class PCHPlanningAlgorithm extends BasePlanningAlgorithm {

	private class TaskComparator implements Comparator<Task> {

		@Override
		public int compare(Task o1, Task o2) {
			if (P.get(o1) > P.get(o2))
				return -1;
			else if (P.get(o1) < P.get(o2))
				return 1;
			else if (o1.getCloudletId() < o2.getCloudletId())
				return -1;
			else if (o1.getCloudletId() > o2.getCloudletId())
				return 1;
			return 0;
		}

	}

	List<CondorVM> availableVms;

	Map<Task, Double> EST;

	Map<Task, Double> EFT;

	private Map<Vm, List<Task>> allocation;

	List<List<Task>> clusters;

	private Map<Task, Double> P;

	private Map<Vm, Vector<Double>> TIME;

	private Vm bestResource = null;

	private double highestBandwidth = Double.MIN_VALUE;

	private List<Task> entryTasks;

	private List<Task> exitTasks;

	Map<Integer, CondorVM> mId2Vm;

	private PlanningAlgorithm usedByOther;

	public PCHPlanningAlgorithm(PlanningAlgorithm name) {
		this();
		usedByOther = name;
	}

	public PCHPlanningAlgorithm() {
		usedByOther = PlanningAlgorithm.PCH;
		EST = new HashMap<Task, Double>();
		EFT = new HashMap<Task, Double>();
		P = new HashMap<Task, Double>();
		clusters = new ArrayList<List<Task>>();
		allocation = new HashMap<Vm, List<Task>>();
		TIME = new Hashtable<Vm, Vector<Double>>();
		entryTasks = new ArrayList<Task>();
		exitTasks = new ArrayList<Task>();
		mId2Vm = new HashMap<Integer, CondorVM>();
	}

	public void resetCounter(Vm vm) {
		if (!TIME.containsKey(vm)) {
			Vector<Double> v = new Vector<Double>(vm.getNumberOfPes());
			for (int i = 0; i < vm.getNumberOfPes(); i++)
				v.add(i, 0.0);

			TIME.put(vm, v);
		}
		for (int i = 0; i < vm.getNumberOfPes(); i++)
			this.TIME.get(vm).set(i, 0.0);
	}

	private void resetCounter() {
		for (Vm vm : availableVms)
			resetCounter(vm);
	}

	Double getTimeAvailable(Vm vm) {
		if (vm == null)
			return 0.0;
		return Collections.min(TIME.get(vm));
	}

	void setTime(Vm vm, Double time) {
		if (vm != null) {
			int min = 0;
			for (int i = 1; i < vm.getNumberOfPes(); i++)
				if (this.TIME.get(vm).get(i) < this.TIME.get(vm).get(min))
					min = i;
			this.TIME.get(vm).set(min, time);
		}
	}

	public double getMakespan() {
		double max = 0;
		double time = 0.0;

		for (Task exit : exitTasks) {
			time = this.EFT.get(exit);
			if (time > max)
				max = time;
		}

		return max <= 0 ? Double.MAX_VALUE : max;
	}

	/*
	 * looking for the best resource, with hightest mips
	 */
	private Vm getBestResource() {

		double mips = Double.MIN_VALUE;
		Vm best = null;
		for (Vm vm : this.availableVms)
			if (vm.getMips() > mips) {
				best = vm;
				mips = vm.getMips();
			}
		return best;
	}

	/*
	 * looking for the best resource, with hightest banwidth
	 */
	private double getBestBandwidth() {

		double max = Double.MIN_VALUE;
		double bandwidth = 0.0;

		for (Vm vm : this.availableVms) {
			bandwidth = vm.getBw();
			max = Math.max(bandwidth, max);
		}

		return max;
	}

	/*
	 * Priority, where we use the best resource as r in the paper, because it is
	 * the initial values to calculate P
	 */
	private double calculatePriorities(Task i) {
		if (P.get(i) > 0)
			return P.get(i);

		double w = getComputationCost(i, bestResource);

		if (exitTasks.contains(i)) {
			P.put(i, w);
			return w;
		}

		double max, cost, priority;
		max = cost = priority = 0.0;
		for (Task t : i.getChildList()) {
			cost = getCommunicationCost(i, null, t, null);
			priority = calculatePriorities(t);
			max = Math.max(cost + priority, max);
		}

		P.put(i, max + w);
		return max + w;
	}

	/*
	 * Earliest finish time (EFT)
	 */
	double calculateEFT(Task t, Vm v) {
		if (!this.EFT.containsKey(t))
			this.EFT.put(t, 0.0);

		return calculateEST(t, v) + getComputationCost(t, v);

	}

	/*
	 * Earliest start time (EST)
	 */
	double calculateEST(Task i, Vm n) {
		if (!this.EST.containsKey(i))
			this.EST.put(i, 0.0);

		if (entryTasks.contains(i))
			return 0.0;

		double maxEST, currentEST, c, predEST;
		maxEST = currentEST = c = predEST = 0.0;

		for (Task j : i.getParentList()) {
			Vm m = mId2Vm.get(j.getVmId());
			c = getCommunicationCost(j, m, i, n);

			if (m == null)
				predEST = calculateEST(j, m);
			else
				predEST = EST.get(j);

			currentEST = predEST + getComputationCost(j, m) + c;

			if (currentEST > maxEST)
				maxEST = currentEST;
		}

		return maxEST;
	}

	double getComputationCost(Task task, Vm vm) {
		if (vm == null)
			vm = bestResource;
		if (vm.getNumberOfPes() < task.getNumberOfPes())
			return Double.MAX_VALUE;

		return task.getCloudletTotalLength() / vm.getMips();
	}

	private double getCommunicationCost(Task parent, Vm j, Task child, Vm k) {
		double bytes = 0.0;
		double bandwith = 0.0;
		@SuppressWarnings("unchecked")
		List<File> parentFiles = (List<File>) parent.getFileList();
		@SuppressWarnings("unchecked")
		List<File> childFiles = (List<File>) child.getFileList();

		if ((j == null || k == null))
			bandwith = highestBandwidth;
		else if (j.equals(k))
			bandwith = Double.POSITIVE_INFINITY;
		else
			bandwith = Math.min(j.getBw(), k.getBw());

		for (File parentFile : parentFiles) {
			if (parentFile.getType() != Parameters.FileType.OUTPUT.value) {
				continue;
			}

			for (File childFile : childFiles) {
				if (childFile.getType() == Parameters.FileType.INPUT.value
						&& childFile.getName().equals(parentFile.getName())) {
					bytes += childFile.getSize();
					break;
				}
			}
		}

		// Mbps in bps
		bandwith *= (double) Consts.MILLION;
		// bps in Bps, because size of the files is in bytes
		bandwith /= 8;

		// answer in time
		return bytes / bandwith;
	}

	/*
	 * Algorithm 2 of the paper (A performance-oriented adaptive scheduler for
	 * dependent tasks on grids)
	 */
	private List<Task> getNextCluster(PriorityQueue<Task> queue) {

		List<Task> cluster = new ArrayList<Task>();
		Task k = queue.poll();
		cluster.add(k);

		Stack<Task> stack = new Stack<Task>();
		stack.add(k);
		while (!stack.isEmpty()) {
			Task n = stack.pop();
			double highest = 0.0;
			double value = 0.0;
			Task sucessor = null;

			for (Task i : n.getChildList()) {

				if (queue.contains(i)) {
					value = P.get(i) + calculateEST(i, bestResource);
					if (value > highest) {
						highest = value;
						sucessor = i;
					}
				}
			}

			if (sucessor != null) {
				stack.add(sucessor);
				cluster.add(sucessor);
				queue.remove(sucessor);
			}
		}

		return cluster;
	}

	@SuppressWarnings("unchecked")
	private void pch() {

		PriorityQueue<Task> unscheduleTasks = new PriorityQueue<Task>(
				getTaskList().size(), new TaskComparator());

		unscheduleTasks.addAll(getTaskList());
		resetCounter();
		while (unscheduleTasks.size() > 0) {
			List<Task> cluster = getNextCluster(unscheduleTasks);

			clusters.add(cluster);
			Vm bestResource = getBestResourceToCluster(cluster);

			System.out.println("Cluster " + clusters.size() + " : " + cluster
					+ " => " + bestResource);

			allocate(cluster, bestResource);

			/*
			 * Recalculating EST, EFT, Time and Weights
			 */

			resetCounter(bestResource);

			for (Task t : allocation.get(bestResource))
				update(t);
		}

		List<Task> list = getDagSortedByPriority();
		resetCounter();
		for (Task t : list)
			update(t);
	}

	/**
	 * Update the EST and EFT values
	 * 
	 * @param t
	 *            : A task
	 */
	void update(Task t) {
		double EST, computationCost, timeAvailable, EFT;
		Vm vm = mId2Vm.get(t.getVmId());

		EST = calculateEST(t, vm);
		timeAvailable = getTimeAvailable(vm);
		EST = Math.max(EST, timeAvailable);
		computationCost = getComputationCost(t, vm);
		EFT = EST + computationCost;

		setTime(vm, EFT);

		this.EST.put(t, EST);
		this.EFT.put(t, EFT);
	}

	private Vm getBestResourceToCluster(List<Task> cluster) {
		Vm bestResource = null;

		double smallestEstSucessorLastClusterNode = Double.MAX_VALUE;

		Collections.sort(cluster, new TaskComparator());

		Task sucessorLastClusterNode = getSucessorScheduled(cluster.get(cluster
				.size() - 1));
		Task lastClusterTask = cluster.get(cluster.size() - 1);

		double EFTlastClusterTask = 0.0;

		for (Vm vm : availableVms) {
			resetCounter(vm);
			double ESTsucessorLastClusterNode = Double.NEGATIVE_INFINITY;

			/*
			 * allocate the cluster
			 */
			List<Task> newCluster = allocate(cluster, vm);

			/*
			 * Update EFT and EST value for each node in cluster
			 */
			for (Task t : newCluster)
				update(t);

			EFTlastClusterTask = this.EFT.get(lastClusterTask);
			if (sucessorLastClusterNode != null
					&& newCluster.contains(sucessorLastClusterNode))
				ESTsucessorLastClusterNode = this.EST
						.get(sucessorLastClusterNode);

			/*
			 * Critical path
			 */
			if (sucessorLastClusterNode == null) {
				if (EFTlastClusterTask < smallestEstSucessorLastClusterNode) {
					smallestEstSucessorLastClusterNode = EFTlastClusterTask;
					bestResource = vm;
				}
			}

			/*
			 * non-critical path
			 */
			else {

				if (Double.isInfinite(ESTsucessorLastClusterNode))
					ESTsucessorLastClusterNode = EFTlastClusterTask
							+ getCommunicationCost(lastClusterTask, vm,
									sucessorLastClusterNode,
									mId2Vm.get(sucessorLastClusterNode
											.getVmId()));

				if (ESTsucessorLastClusterNode < smallestEstSucessorLastClusterNode) {
					smallestEstSucessorLastClusterNode = ESTsucessorLastClusterNode;
					bestResource = vm;
				}
			}

			/*
			 * desallocate the cluster
			 */
			desallocate(cluster, vm);
		}

		return bestResource;
	}

	private Task getSucessorScheduled(Task task) {
		for (Task sucessor : task.getChildList()) {
			if (mId2Vm.get(sucessor.getVmId()) != null)
				return sucessor;
		}
		return null;
	}

	private void printPriorities() {
		System.out.println("Priorities: ");
		List<Task> cp = new ArrayList<Task>(getTaskList());
		Collections.sort(cp, new TaskComparator());

		for (Object o : cp) {
			Task t = (Task) o;
			System.out.println(t + "=> P:" + this.P.get(t));
		}
		System.out.println("");
	}

	private List<Task> allocate(List<Task> cluster, Vm vm) {
		if (!allocation.containsKey(vm))
			allocation.put(vm, new ArrayList<Task>());

		for (Task t : cluster) {
			t.setVmId(vm.getId());
			if (!allocation.get(vm).contains(t))
				allocation.get(vm).add(t);
		}

		Collections.sort(allocation.get(vm), new TaskComparator());

		return allocation.get(vm);

	}

	List<Task> allocate(Task task, Vm vm) {
		if (!allocation.containsKey(vm))
			allocation.put(vm, new ArrayList<Task>());

		task.setVmId(vm.getId());

		if (!allocation.get(vm).contains(task))
			allocation.get(vm).add(task);
		Collections.sort(allocation.get(vm), new TaskComparator());

		return allocation.get(vm);
	}

	private List<Task> desallocate(List<Task> cluster, Vm vm) {
		for (Task t : cluster)
			t.setVmId(-1);

		allocation.get(vm).removeAll(cluster);
		Collections.sort(allocation.get(vm), new TaskComparator());

		return allocation.get(vm);
	}

	List<Task> desallocate(Task task, Vm vm) {
		if (vm == null)
			return null;

		task.setVmId(-1);
		allocation.get(vm).remove(task);

		return allocation.get(vm);
	}

	@Override
	public void run() throws Exception {
		System.out.println("\n---Begin PCH---\n");
		this.availableVms = new ArrayList<CondorVM>();

		switch (usedByOther) {
		case HCOC:
			/*
			 * get private cloud only!
			 */
			for (Object o : getVmList()) {
				CondorVM v = (CondorVM) o;
				if (v.getPrice() < WorkflowSimTags.EPSILON)
					this.availableVms.add(v);
			}

			/*
			 * if hybrid cloud = public cloud only
			 */
			if (this.availableVms.size() <= 0)
				return;
			break;

		default:
			this.availableVms.addAll(getVmList());
			break;
		}

		for (Object objectTask : getTaskList()) {
			Task task = (Task) objectTask;
			if (task.getParentList().isEmpty())
				entryTasks.add(task);
			else if (task.getChildList().isEmpty())
				exitTasks.add(task);
		}

		for (int i = 0; i < getVmList().size(); i++) {
			CondorVM vm = (CondorVM) getVmList().get(i);
			if (vm != null) {
				mId2Vm.put(vm.getId(), vm);
			}
		}

		/*
		 * Looking for the best resource
		 */
		this.bestResource = getBestResource();

		/*
		 * Looking for the highest bandwidth
		 */
		this.highestBandwidth = getBestBandwidth();

		/*
		 * calculate tasks priorities (the bigger the task chain, bigger the
		 * priority)
		 */
		for (Object o : getTaskList()) {
			Task t = (Task) o;
			P.put(t, -1.0);
		}

		for (Task entry : entryTasks) {
			calculatePriorities(entry);
		}
		printPriorities();

		/*
		 * Inicializing Time hashtable
		 */
		resetCounter();

		/*
		 * Calculate EFT and indirect EST
		 */
		for (Object o : getTaskList()) {
			Task t = (Task) o;
			calculateEFT(t, bestResource);
		}

		pch();

		System.out.println("\nPCH Result:");
		for (Object o : getTaskList()) {
			Task task = (Task) o;

			Vm vm = mId2Vm.get(task.getVmId());
			System.out.println(vm + " -> " + task + " : " + EST.get(task)
					+ " + " + getComputationCost(task, vm) + " = "
					+ EFT.get(task));
		}
		System.out.println("makespan: " + getMakespan());
		System.out.println("\n---END PCH---\n");

	}

	public List<Task> getDagSortedByPriority() {
		@SuppressWarnings("unchecked")
		List<Task> list = new ArrayList<Task>(getTaskList());
		Collections.sort(list, new TaskComparator());
		return list;
	}

	public List<Task> getAllocation(Vm vm) {
		if (allocation.containsKey(vm))
			return allocation.get(vm);
		return null;
	}

}
