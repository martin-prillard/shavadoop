package fr.telecompt.shavadoop.tasktracker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import fr.telecompt.shavadoop.util.Constant;

public class ListenerFinishedTaskSlave extends Thread {

	private StateSlaveManager checkSlaveState;
	private Socket socket;
	
	public ListenerFinishedTaskSlave(StateSlaveManager _checkSlaveState, Socket _socket){
		checkSlaveState = _checkSlaveState;
		socket = _socket;
	}
	
	public void run() {
		
		try {	
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));	
			
			while(!checkSlaveState.getTaskFinished()) {
				
				String slaveAliveString = in.readLine();
				System.out.println(">>>listener : " + slaveAliveString); //TODO
				
				if (slaveAliveString.equalsIgnoreCase(Constant.ANSWER_TASKTRACKER_REQUEST_TASK_FINISHED)) {
					checkSlaveState.caseWorkerTaskIsFinished();
				}
			}

		} catch (IOException e) {e.printStackTrace();}

	}
}
