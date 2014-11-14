package fr.telecompt.shavadoop.tasktracker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import fr.telecompt.shavadoop.util.Constant;

public class GetSlaveState extends Thread {

	private ServerSocket ss;
	private CheckSlaveState checkSlaveState;
	
	public GetSlaveState(CheckSlaveState _checkSlaveState, ServerSocket _ss) {
		checkSlaveState = _checkSlaveState;
		ss = _ss;
	}
	
	public void run() {
		
		BufferedReader in = null;
        Socket socket = null;
        
		try {
			socket = ss.accept();
			
			// send request
			sendRequestStateSlave(socket);
			
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        
	        // get the state slave
	        String slaveAliveString = in.readLine();
	        System.out.println(">>>" + slaveAliveString);
	        boolean slaveAlive = false;
	        if (slaveAliveString.equalsIgnoreCase(Constant.ANSWER_TASKTRACKER_REQUEST_OK)) {
	        	slaveAlive = true;
	        } else if (slaveAliveString.equalsIgnoreCase(Constant.ANSWER_TASKTRACKER_REQUEST_KO)) {
	        	slaveAlive = false;
	        }
	        checkSlaveState.setStateSlave(slaveAlive);
	        

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
            try {
                socket.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

	}
	
	/**
	 * Send request to the slave to know his state
	 * @param socket
	 */
	public void sendRequestStateSlave(Socket socket) {
		// request slave state
	    PrintWriter out = null;
	    try {
	        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            	
            // send state
	        out.println(Constant.MESSAGE_TASKTRACKER_REQUEST);
	        out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
