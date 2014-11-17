package fr.telecompt.shavadoop.tasktracker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class StateSlave extends Thread {

	static Logger log = Logger.getLogger(StateSlave.class.getName());
	 
	private int portTaskTracker;
	private String hostMaster;
	private boolean run = true;
	private Slave slave;
	
	public StateSlave(Slave _slave, String _hostMaster, int _portTaskTracker) {
		slave = _slave;
		portTaskTracker = _portTaskTracker;
		hostMaster = _hostMaster;
	}
	
	public void stopStateSlave() {
		run = false;
	}
	
	public void run() {
		
		while (run) {
			try {
				Socket socket = new Socket(hostMaster, portTaskTracker);
				waitRequestMaster(socket);
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
				if (Constant.MODE_DEBUG) System.out.println("Stdout slave : " + e.getMessage()); //TODO
			}
		}
		
	}
	
	public void waitRequestMaster(Socket socket) {

		//Get the return message from the server
		BufferedReader in = null;
        
		try {
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        
	        // get the request from the task tracker
	        String request = in.readLine();
	        
	        if (request.equalsIgnoreCase(Constant.MESSAGE_TASKTRACKER_REQUEST)) {
	        	// if the task is already finished
		        if (slave.isTaskFinished()) {
		        	sendTaskFinished();
		        } else {
		        	// send the slave state
		        	sendState(socket, slave.isState());
		        }
	        }
	        
		} catch (Exception e) {
			e.printStackTrace();
			if (Constant.MODE_DEBUG) System.out.println("Stdout slave : " + e.getMessage()); //TODO
			run = false;
		}
	}
	
	/**
	 * Send if the worker is working or not
	 * @param state
	 */
	public void sendState(Socket socket, boolean state) {
		
		// request slave state
	    PrintWriter out = null;
	    
	    try {
	        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            	
            // send state
            String stateString = null;
	        if (state) {
	        	stateString = Constant.ANSWER_TASKTRACKER_REQUEST_OK;
	        } else {
	        	stateString = Constant.ANSWER_TASKTRACKER_REQUEST_KO;
	        }
			out.println(stateString);
			out.flush();
			
		} catch (IOException e) {
			e.printStackTrace();
			if (Constant.MODE_DEBUG) System.out.println("Stdout slave : " + e.getMessage()); //TODO
			run = false;
		} 
	}
	
	
	public void sendTaskFinished() {
		// request slave state
	    PrintWriter out = null;
	    try {
			Socket socket = new Socket(hostMaster, portTaskTracker);
	        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
			out.println(Constant.ANSWER_TASKTRACKER_REQUEST_TASK_FINISHED);
			out.flush();
			socket.close();
			run = false;
		} catch (IOException e) {
			e.printStackTrace();
			if (Constant.MODE_DEBUG) System.out.println("Stdout slave : " + e.getMessage()); //TODO
		} 
	}
	
}
