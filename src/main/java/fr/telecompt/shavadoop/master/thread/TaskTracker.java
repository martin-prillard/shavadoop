package fr.telecompt.shavadoop.master.thread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class TaskTracker extends Thread {

	private final int FREQ_CHECK_ALIVE = 2500;
	
	private ExecutorService es;
	private String hostMaster;
	private List<List<String>> taskHistory = new ArrayList<List<String>>();
	private SSHManager sm;
	private Map<String, String> dictionaryFile = null;
	
	public TaskTracker(SSHManager _sm, String _hostMaster, ExecutorService _es, Map<String, String> _dictionaryFile) {
		sm = _sm;
		hostMaster = _hostMaster;
		es = _es;
		dictionaryFile = _dictionaryFile;
	}
	
	/**
	 * Add worker
	 * @param host
	 * @param taskName
	 * @param fileToTreat
	 * @param key
	 */
	public void addTask(String host, String taskName, String fileToTreat, String key) {
		List<String> taskInfos = new ArrayList<String>();
		taskInfos.add(host);
		taskInfos.add(taskName);
		taskInfos.add(fileToTreat);
		taskInfos.add(key);
		taskHistory.add(taskInfos);
	}
	
	public void run() {
		check();
	}
	
	/**
	 * Check if the workers are alive
	 */
	public void check() {
		if (Constant.APP_DEBUG) System.out.println("TASK_TRACKER : START");
		Iterator<List<String>> it = taskHistory.iterator();
		
		while (!es.isTerminated()) {
			if (!es.isTerminated()) {
				if(it.hasNext()) {
					
					List<String> task = it.next();
					String host = task.get(0);
					String nameTask = task.get(1);
					String fileTask = task.get(2);
					String key = task.get(3);
					
					// if the distant worker is dead
					if (!sm.isLocal(host) && !sm.isAlive(host)) {
						
						if (Constant.APP_DEBUG) System.out.println("TASK_TRACKER : " + host + " died"); 
						
						// we get an other worker
						List<String> hostWorker = sm.getHostAliveCores(1);
						if (hostWorker.size() == 1) {
							host = hostWorker.get(0);
						} else {
							// it's the master
							host = sm.getHostMasterFull();
						}
						// we relaunch the task on a over worker
						if (!es.isTerminated()) {
							relaunchTask(host, nameTask, fileTask, key);
							if (Constant.APP_DEBUG) System.out.println("TASK_TRACKER : redirect " + nameTask + " task on " + host); 
						}
					} else {
						if (Constant.APP_DEBUG) System.out.println("TASK_TRACKER : " + host + " alive");
					}
				} else {
					// reset iterator
					it = taskHistory.iterator();
				}
				// wait before check an other worker
		    	try {
		    	    Thread.sleep(FREQ_CHECK_ALIVE);
		    	} catch(InterruptedException ex) {
		    	    Thread.currentThread().interrupt();
		    	}
			}
		}
		if (Constant.APP_DEBUG) System.out.println("TASK_TRACKER : END");
	}
	
	public void relaunchTask(String host, String taskName, String fileTask, String key) {
		// add this new task
		addTask(host, taskName, fileTask, key);
		// if needed, modify the dictionary file
		if (dictionaryFile != null) {
			// erase old information of the worker failed
			dictionaryFile.put(key, host);
		}
		// launch the task
		switch(taskName){
			case Slave.SPLIT_MAPPING_FUNCTION:
				es.execute(new LaunchSplitMapping(sm.isLocal(host), sm.getDsaKey(), host, fileTask, hostMaster));
				break;
			case Slave.SHUFFLING_MAP_FUNCTION:
				es.execute(new LaunchShufflingMap(sm.isLocal(host), sm.getDsaKey(), host, fileTask, hostMaster));
				break;
		}
	}
}
