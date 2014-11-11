package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.thread.FileTransfert;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

public class SSHManager {
	
	private List<String> hostsNetwork;
	private int shellPort = 0;
	private int cores = Runtime.getRuntime().availableProcessors();
	private String fileIpAdress = null;
	private String dsaFile = null;
	private String dsaKey = null;
	private PropReader prop = new PropReader();
	private String host;
	private String hostFull;
	private String username = System.getProperty("user.name");
	private String homeDirectory = System.getProperty("user.home");
	
	public void initialize() {
		shellPort = Integer.parseInt(prop.getPropValues(PropReader.PORT_SHELL));
		
		dsaFile = prop.getPropValues(PropReader.FILE_DSA);
		if (dsaFile == null || dsaFile.isEmpty() || dsaFile.equalsIgnoreCase("")) {
			dsaFile = homeDirectory + Constant.DSA_DEFAULT_FILE;
		}
		
		fileIpAdress = Constant.NETWORK_IP_FILE;
		
		try {
			hostFull = InetAddress.getLocalHost().getCanonicalHostName();
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {e.printStackTrace();}

		// get the list of hosts of the network
		hostsNetwork = getHostFromFile();
		
    	// get dsa key
		dsaKey = getDsaKey(dsaFile);
	}
	
	/**
	 * Return x hosts alive
	 * @param hosts
	 * @param nbHost
	 * @param shellPort
	 * @param usernameMaster
	 * @param dsaKey
	 * @return
	 */
	public List<String> getHostAliveCores(int nbWorker) {
		
		if (Constant.MODE_DEBUG) System.out.println("Search " + nbWorker + " worker(s) alive...");
		
		List<String> hostAlive = new ArrayList<String>();
		
		// check first for this computer : the master is the worker
		for (int i = 0; i < cores; i++) {
			// the number of worker is the number of cores of this computer
			if (hostAlive.size() < nbWorker) {
				hostAlive.add(hostFull);
			} else {
				break;
			}
		}
		
		String destFile = Constant.APP_PATH_SLAVE + Constant.APP_JAR;
		File jar = new File(destFile);
		
		// if need more worker, use the distant computer
		if (hostAlive.size() < nbWorker) {
			for (String host : hostsNetwork) {
				if (hostAlive.size() < nbWorker) {
					if (isAlive(host)) {
						for (int i = 0; i < getCoresNumber(host); i++) {
							if (hostAlive.size() < nbWorker) {
								// add to our list of cores alive
								hostAlive.add(host);
								// transfert the jar program if needed
								if (!jar.exists()) {
									FileTransfert ft = new FileTransfert(this, host, Constant.APP_PATH_JAR, destFile);
									ft.transfertFileScp();
								}
							} else {
								break;
							}
						}
					}
				} else {
					break;
				}
			}
		}
		
		if (Constant.MODE_DEBUG) System.out.println(hostAlive.size() + " worker(s) alive found !");
		
		return hostAlive;
	}
	
	/**
	 * Test if a host is alive
	 * @param host
	 * @param shellPort
	 * @param usernameMaster
	 * @param dsaKey
	 * @return true if it's alive
	 */
	public boolean isAlive(String host) {
		boolean alive = false;
		// test if this host is alive
		try {
			//Connect to the distant computer
			Shell shell = new SSH(host, shellPort, Constant.USERNAME_MASTER, dsaKey);
			new Shell.Plain(shell).exec("echo " + host); 
			alive = true;
		} catch (Exception e) {
			// System.out.println("Fail to connect to " + host);
		}
		return alive;
	}
	
	/**
	 * Return the cores number from the distant computer
	 * @param host
	 * @return cores
	 */
	public int getCoresNumber(String host) {
		int cores = 1;
		// test if this host is alive
		try {
			// connect to the distant computer
			Shell shell = new SSH(host, shellPort, Constant.USERNAME_MASTER, dsaKey);
			// get the number of cores
			String cmd = "grep -c ^processor /proc/cpuinfo";
			String stdout = new Shell.Plain(shell).exec(cmd);
			cores = Integer.parseInt(stdout.trim()); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cores;
	}
	
    /**
     * Return list of hostname from a file
     * @param fileIpAdress
     * @return
     */
    public List<String> getHostFromFile() {
    	List<String> hostnameMappers = new ArrayList<String>();

		 try {
             FileReader fic = new FileReader(fileIpAdress);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             
             while ((line = read.readLine()) != null) {
            	 hostnameMappers.add(line);
             }
             fic.close();
             read.close();   
             
         } catch (IOException e) {
             e.printStackTrace();
         }
		 
    	return hostnameMappers;
    }
    
    /**
     * Return the dsa key
     * @param dsaFile
     * @return dsa key
     */
	public String getDsaKey(String dsaFile) {
		String dsaKey = "";	

		try {
			InputStream ips=new FileInputStream(dsaFile); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String line;
			while((line=br.readLine())!=null){
				dsaKey += line + "\n";
			}
			br.close();
			if (Constant.MODE_DEBUG) System.out.println("Dsa key found");
		} catch (IOException e) {
			System.out.println("No dsa file");
		}

		return dsaKey;
	}

	public String getDsaKey() {
		return dsaKey;
	}

	public String getHostFull() {
		return hostFull;
	}
	
	public int getShellPort() {
		return shellPort;
	}

	public String getUsername() {
		return username;
	}

	public String getHost() {
		return host;
	}

	public static void generateNetworkIpAdress(String regex) {
		
//		String cmd = "cat /proc/net/arp | grep -o \"" + prop.getPropValues(PropReader.NETWORK_IP_REGEX) + "\""; //TODO remove
		String cmd = "arp -a | grep -o \"" + regex + "\""; //TODO don't work. Why ??

		try {
			String line;
			// Run a java app in a separate system process
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			
			List<String> ipAdress = new ArrayList<String>();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()) );
			while ((line = in.readLine()) != null) {
				if (Constant.MODE_DEBUG) System.out.println("On local : " + line);
				ipAdress.add(line);
			}
			in.close();
			
			if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
			Util.writeFile(Constant.NETWORK_IP_DEFAULT_FILE, ipAdress);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public boolean isLocal(String worker) {
		boolean local = false;
		if (worker.equalsIgnoreCase(hostFull)) {
			// the worker is the master
			local = true;
		}
		return local;
	}
}
