package fr.telecompt.shavadoop.tasktracker;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.thread.LaunchShufflingMap;
import fr.telecompt.shavadoop.thread.LaunchSplitMapping;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.Pair;

public class TaskTracker extends Thread {

	private ExecutorService es;
	private String hostMaster;
	private Map<Thread, List<String>> taskHistory = new HashMap<Thread, List<String>>();
	private SSHManager sm;
	private Map<String, Pair> dictionaryReducing = null;
	private int portTaskTracker;
	private Iterator<Entry<Thread, List<String>>> it;
	
	public TaskTracker(SSHManager _sm, ExecutorService _es, int _portTaskTracker, Map<String, Pair> _dictionaryReducing) {
		sm = _sm;
		hostMaster = sm.getHost();
		portTaskTracker = _portTaskTracker;
		es = _es;
		dictionaryReducing = _dictionaryReducing;
	}
	
	/**
	 * Add worker
	 * @param host
	 * @param taskName
	 * @param fileToTreat
	 * @param key
	 */
	public void addTask(Thread thread, String host, String idWorker, String taskName, String fileToTreat, String key) {
		List<String> taskInfos = new ArrayList<String>();
		taskInfos.add(host);
		taskInfos.add(idWorker);
		taskInfos.add(taskName);
		taskInfos.add(fileToTreat);
		taskInfos.add(key);
		taskHistory.put(thread, taskInfos);
		it = taskHistory.entrySet().iterator();
	}
	
	public void removeTask(Thread thread) {
		taskHistory.remove(thread);
		it = taskHistory.entrySet().iterator();
	}
	
	public void run() {
		check();
	}
	
	/**
	 * Check if the workers are alive
	 */
	public void check() {
		
		if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : START");
		
		it = taskHistory.entrySet().iterator();
		ServerSocket ss = null;
		
		
		try {
			ss = new ServerSocket(portTaskTracker);
			ss.setReuseAddress(true);
		} catch (IOException e) {e.printStackTrace();}
		
		while (!es.isTerminated()) {
			
			if (!es.isTerminated()) {
				if(it.hasNext()) {
					Map.Entry<Thread, List<String>> task = (Map.Entry<Thread, List<String>>)it.next();
					// launch process to check the state slave
					String host = task.getValue().get(0);
					// if it's not the local worker
					if (!sm.isLocal(host)) {
						new CheckSlaveState(this, es, ss, sm, task).start();
					} else {
						if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : " + host + " (local) alive"); //TODO supr
					}
				} else {
					// reset iterator
					it = taskHistory.entrySet().iterator();
				}
				// wait before check an other worker
		    	try {
		    	    Thread.sleep(Constant.TASK_TRACKER_FREQ);
		    	} catch(InterruptedException ex) {
		    	    Thread.currentThread().interrupt();
		    	}
			}
		}
		
		try {
			ss.close();
		} catch (IOException e) {e.printStackTrace();}
		
		if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : END");
	}
	
	public void relaunchTask(Thread thread, String newHost, String idWorker, String taskName, String fileTask, String key) {
		// if needed, modify the dictionary file
		if (dictionaryReducing != null) {
			// erase old information of the worker failed
			Pair newHostFull = new Pair(newHost, idWorker);
			dictionaryReducing.put(key, newHostFull);
		}
		// launch the task
		Thread newTask = null;
		switch(taskName){
			case Slave.SPLIT_MAPPING_FUNCTION:
				newTask = new LaunchSplitMapping(sm, newHost, fileTask, sm.isLocal(newHost), hostMaster, idWorker);
				es.execute(newTask);
				break;
			case Slave.SHUFFLING_MAP_FUNCTION:
				newTask = new LaunchShufflingMap(sm, newHost, fileTask, hostMaster, idWorker);
				es.execute(newTask);
				break;
		}
		
		// add this new task
		addTask(newTask, newHost, idWorker, taskName, fileTask, key);
		
		//interrupt the old thread
		thread.interrupt();
		
		//remove from the map
		removeTask(thread);
	}
}
