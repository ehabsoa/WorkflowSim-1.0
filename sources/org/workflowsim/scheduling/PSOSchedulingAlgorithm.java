package org.workflowsim.scheduling;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.planning.BasePlanningAlgorithm;
import org.workflowsim.planning.PSOPlanningAlgorithm;
import org.workflowsim.utils.Parameters.ClassType;

public class PSOSchedulingAlgorithm extends BaseSchedulingAlgorithm {

	private PSOPlanningAlgorithm planner;

	public PSOSchedulingAlgorithm(BasePlanningAlgorithm planner) {
		this.planner = (PSOPlanningAlgorithm) planner;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() throws Exception {

		if (!getCloudletList().isEmpty()) {

			Map<Integer, CondorVM> mId2Vm = new HashMap<Integer, CondorVM>();

			for (int i = 0; i < getVmList().size(); i++) {
				CondorVM vm = (CondorVM) getVmList().get(i);
				if (vm != null) {
					mId2Vm.put(vm.getId(), vm);
				}
			}

			if (!getCloudletList().isEmpty()) {
				Job job = (Job) getCloudletList().get(0);
				if (job.getClassType() != ClassType.STAGE_IN.value)
					planner.doOnline(getCloudletList());
			}

			int size = getCloudletList().size();

			for (int i = 0; i < size; i++) {
				Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
				// System.out.println("Cloudletid: "+cloudlet.getCloudletId());
				/**
				 * Make sure cloudlet is matched to a VM. It should be done in
				 * the Workflow Planner. If not, throws an exception because
				 * StaticSchedulingAlgorithm itself does not do the mapping.
				 */

				if (cloudlet.getVmId() < 0
						|| !mId2Vm.containsKey(cloudlet.getVmId())) {
					Log.printLine("Cloudlet " + cloudlet.getCloudletId()
							+ " is not matched."
							+ "It is possible a stage-in job");
					cloudlet.setVmId(0);

				}

				CondorVM vm = (CondorVM) mId2Vm.get(cloudlet.getVmId());

				if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
					vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
					getScheduledList().add(cloudlet);
					Log.printLine("Schedules " + cloudlet.getCloudletId()
							+ " with " + cloudlet.getCloudletLength()
							+ " to VM " + cloudlet.getVmId());
				}
			}
		}
	}

}
