package fr.telecompt.shavadoop.tasktracker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.util.Constant;

public class StateSlaveManager extends Thread {

	private ServerSocket ss;
	private TaskTracker ts;
	private Thread thread;
	private List<String> taskList;
	private SSHManager sm;
	private ExecutorService es;
	private boolean taskFinished = false;
	private String host;
	private String idWorker;
	private String taskName;
	private String fileTask;
	private String key;
	
	public StateSlaveManager(TaskTracker _ts, ExecutorService _es, ServerSocket _ss, SSHManager _sm, Thread _taskThread, List<String> _taskList){
		ts = _ts;
		es = _es;
		ss = _ss;
		sm = _sm;
		thread = _taskThread;
		taskList = _taskList;
		
		host = taskList.get(0);
		idWorker = taskList.get(1);
		taskName = taskList.get(2);
		fileTask = taskList.get(3);
		key = taskList.get(4);
	}
	
	public void run() {
		
		// the distant worker is dead
		if (!sm.isLocal(host) && !sm.isAlive(host)) {
			caseWorkerDied();
		// if the worker is alive
		} else {
			
			Socket socket;
			
			try {
				socket = ss.accept();
				
				ExecutorService esStateSlaveManager = Executors.newCachedThreadPool();
				
//				new ListenerFinishedTaskSlave(this, socket); //TODO
				
				while(!taskFinished) {
					// we trie to send request to the slave to know if it's alive or not
			    	if (!taskFinished) {
			    		esStateSlaveManager.execute(new CheckStateSlave(this, socket)); 
			    	}
					// wait between two requests check
			    	try {
			    	    Thread.sleep(Constant.TASK_TRACKER_FREQ);
			    	} catch(InterruptedException ex) {
			    	    Thread.currentThread().interrupt();
			    	}
				}
				
				esStateSlaveManager.shutdown();
				
				try {
					esStateSlaveManager.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
				} catch (InterruptedException e) {e.printStackTrace();}
				
				socket.close();
				
			} catch (Exception e) {e.printStackTrace();}
		}
    	
	}
	
	/**
	 * In the case where the worker task is finished
	 * @param thread
	 */
	public void caseWorkerTaskIsFinished() {
		taskFinished = true;
		ts.removeTask(thread);
	}
	
	/**
	 * In the case where the worker is dead
	 */
	public void caseWorkerDied() {
		if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : worker " + idWorker + " (" + host + ") died");
		
		String hostFail = host;
		
		// we get an other worker
//		List<String> hostWorker = sm.getHostAliveCores(1);
//		if (hostWorker.size() == 1) { //TODO pour test
//			host = hostWorker.get(0);
//		} else {
			// it's the master
			host = sm.getHostFull();
//		}
		// we relaunch the task on an other worker
		if (!es.isTerminated()) {
			if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : redirect " + taskName + " from worker " + idWorker + " (" + hostFail + ") task on " + host); 
			ts.relaunchTask(thread, host, idWorker, taskName, fileTask, key);
		}
	}

	
	public boolean getTaskFinished() {
		return this.taskFinished;
	}
	
}
