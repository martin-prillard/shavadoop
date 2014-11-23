package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.network.FileTransfert;
import fr.telecompt.shavadoop.network.SSHManager;
import fr.telecompt.shavadoop.network.ShellThread;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

/**
 * 
 * @author martin prillard
 *
 */
public class LaunchSplitMapping extends ShellThread {
	
	private String hostMapper;
	private boolean local;
	private String idWorker;
	private String nbWorker;
	
	
	public LaunchSplitMapping(SSHManager _sm, String _nbWorker, String _distantHost, String _fileToTreat, boolean _local, String _hostMapper, String _idWorker) {
		super(_sm, _distantHost, _fileToTreat);
		nbWorker = _nbWorker;
		hostMapper = _hostMapper;
		local = _local;
		idWorker = _idWorker;
	}
	
	
	public void run() {
		try {
			String pathJar = Constant.PATH_JAR_MASTER;
			String method = Slave.SPLIT_MAPPING_FUNCTION;
	
			// execute on the master's computer
			if(local) {
				// Run a java app in a separate system process
				String cmd = getCmdJar(pathJar, nbWorker, hostMapper, method, fileToTreat, idWorker);
				Process p = Runtime.getRuntime().exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
		        BufferedReader stdOut=new BufferedReader(new InputStreamReader(p.getInputStream()));
		        while((stdOut.readLine())!=null){
		            // do nothing, wait needed for scp
		        }
	            p.waitFor();
	            p.destroy();
			// execute on a distant computer
			} else {
				ExecutorService es = Executors.newCachedThreadPool();
				
				// connect to the distant computer
				shell = new SSH(distantHost, shellPort, Constant.USERNAME, dsaKey);
				
				// master file DSM -> slave
				String destFile = Constant.PATH_REPO_RES 
						+ FilenameUtils.getBaseName(fileToTreat);
				es.execute(new FileTransfert(sm, distantHost, fileToTreat, destFile, true, false));
				fileToTreat = destFile;
				
				String cmd = getCmdJar(pathJar, nbWorker, hostMapper, method, fileToTreat, idWorker);
				
		    	es.shutdown();
				try {
					es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				// launch map process
				new Shell.Plain(shell).exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On " + distantHost + " : " + cmd);
			}
	    } catch (InterruptedIOException e) { // if thread was interrupted
	        Thread.currentThread().interrupt();
	        if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : worker failed was interrupted");
	    } catch (Exception e) {
	        if (!isInterrupted()) { // if other exceptions
	        	System.out.println("Fail to launch shavadoop slave from " + distantHost);
	        } else { 
	        	if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : worker failed was interrupted");
	        }
	    }
	}

}