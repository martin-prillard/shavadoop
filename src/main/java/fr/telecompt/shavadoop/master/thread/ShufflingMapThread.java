package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class ShufflingMapThread extends ShellThread {

	private String key;
	
	public ShufflingMapThread(String _dsaKey, String _hostname, String _fileToTreat, String _key) {
		super(_dsaKey, _hostname, _fileToTreat);
		key = _key;
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			//Launch map process
			String pathJar = Constant.APP_FULL_PATH;
			String method = Slave.SHUFFLING_MAP_FUNCTION;
			
			if (Constant.APP_DEBUG) System.out.println("Cmd : " 
					+ "java -jar " 
					+ pathJar 
					+ " " 
					+ method 
					+ " " 
					+ fileToTreat
					+ " "
					+ key);
			
			new Shell.Plain(shell).exec("java -jar " 
				+ pathJar 
				+ " " 
				+ method 
				+ " " 
				+ fileToTreat
				+ " "
				+ key);
			
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
	
}
