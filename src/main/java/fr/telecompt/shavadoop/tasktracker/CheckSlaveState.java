package fr.telecompt.shavadoop.tasktracker;

import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.util.Constant;

public class CheckSlaveState extends Thread {

	private final int TIMEOUT_CHECK_ALIVE = 60000;
	private ServerSocket ss;
	private TaskTracker ts;
	private Map.Entry<Thread, List<String>> task;
	private SSHManager sm;
	private ExecutorService es;
	private boolean stateSlave = false;
	
	public CheckSlaveState(TaskTracker _ts, ExecutorService _es, ServerSocket _ss, SSHManager _sm, Map.Entry<Thread, List<String>> _task){
		ts = _ts;
		es = _es;
		ss = _ss;
		sm = _sm;
		task = _task;
	}
	
	public void run() {
		
		Thread thread = task.getKey();
		String host = task.getValue().get(0);
		String idWorker = task.getValue().get(1);
		String taskName = task.getValue().get(2);
		String fileTask = task.getValue().get(3);
		String key = task.getValue().get(4);
		
		// the distant worker is dead
		if (!sm.isAlive(host)) {
			caseDistantWorkerDied(thread, host, idWorker, taskName, fileTask, key);
			
		// if the distant worker is alive
		} else {
			// we trie to send request to the slave to know if it's alive or not
			ExecutorService es = Executors.newCachedThreadPool();
			es.execute(new GetSlaveState(this, ss)); 
	    	es.shutdown();
			try {
				es.awaitTermination(TIMEOUT_CHECK_ALIVE, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// the distant worker is dead
			if (!stateSlave) {
				caseDistantWorkerDied(thread, host, idWorker, taskName, fileTask, key);
			} else {
				if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : " + host + " alive");
			}
		}

	}
	
	/**
	 * In the case where the distant worker is dead
	 */
	public void caseDistantWorkerDied(Thread thread, String host, String idWorker, String taskName, String fileTask, String key) {
		if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : " + host + " died"); 
		
		String hostFail = host;
		
		// we get an other worker
		List<String> hostWorker = sm.getHostAliveCores(1);
//		if (hostWorker.size() == 1) { //TODO pour test
//			host = hostWorker.get(0);
//		} else {
			// it's the master
			host = sm.getHostFull();
//		}
		// we relaunch the task on an other worker
		if (!es.isTerminated()) {
			if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : redirect " + taskName + " (" + hostFail + ") task on " + host); 
			ts.relaunchTask(thread, host, idWorker, taskName, fileTask, key);
		}
	}

	public void setStateSlave(boolean stateSlave) {
		this.stateSlave = stateSlave;
	}
	
}
