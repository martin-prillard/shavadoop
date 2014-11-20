package fr.telecompt.shavadoop.network;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

/**
 * 
 * @author martin prillard
 *
 */
public class SSHManager {
	
	private List<String> hostsNetwork;
	private int shellPort = 0;
	private String fileIpAdress = null;
	private String dsaFile = null;
	private String dsaKey = null;
	private PropReader prop = new PropReader();
	private String host;
	private String hostFull;
	private String username = System.getProperty("user.name");
	private String homeDirectory = System.getProperty("user.home");
	private String ipAdress;
	private Set<String> initializedHost = new HashSet<String>();
	
	
	/**
	 * Initialize the SSH manager
	 */
	public void initialize() {
		
		if (Constant.MODE_DEBUG) System.out.println("Initialize SSH Manager :");

		shellPort = Integer.parseInt(prop.getPropValues(PropReader.PORT_SHELL));
		
		dsaFile = prop.getPropValues(PropReader.FILE_DSA);
		if (dsaFile == null || dsaFile.isEmpty() || dsaFile.trim().equalsIgnoreCase("")) {
			dsaFile = homeDirectory + Constant.PATH_DSA_DEFAULT_FILE;
		}
		
		fileIpAdress = Constant.PATH_NETWORK_IP_FILE;
		
		try {
			hostFull = InetAddress.getLocalHost().getCanonicalHostName();
			host = InetAddress.getLocalHost().getHostName();
			ipAdress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {e.printStackTrace();}
		
    	// get dsa key
		dsaKey = getDsaKeyContent(dsaFile);
	}
	
	
	/**
	 * Return x hosts alive
	 * @param nbWorker
	 * @param random
	 * @return list host's cores alive
	 */
	public List<String> getHostAliveCores(int nbWorker, boolean random) {
		
		if (hostsNetwork == null) {
			// get the list of hosts of the network
			hostsNetwork = getHostFromFile(random);
		}
	
		if (Constant.MODE_DEBUG) System.out.println("Search " + nbWorker + " worker(s) alive...");
		
		List<String> hostAlive = new ArrayList<String>();
		
		ExecutorService es = Executors.newCachedThreadPool();
		
		// if need more worker, use the distant computer
		for (String host : hostsNetwork) {
			if (hostAlive.size() < nbWorker) {
				if (isLocal(host)) {
					// add to our list of cores alive
					hostAlive.add(host);
					if (!initializedHost.contains(host)) {
						initializedHost.add(host);
						es.execute(new LaunchInitializeHost(this, es, host));
					}
				} else if (!isLocal(host) && isAlive(host)) {
					for (int i = 0; i < getCoresNumber(host); i++) {
						if (hostAlive.size() < nbWorker) {
							// add to our list of cores alive
							hostAlive.add(host);
							if (!initializedHost.contains(host)) {
								initializedHost.add(host);
								es.execute(new LaunchInitializeHost(this, es, host));
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
		
		es.shutdown();
		
		try {
			es.awaitTermination(Constant.THREAD_MAX_LIFETIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {e.printStackTrace();}
		
		
		if (Constant.MODE_DEBUG) System.out.println(hostAlive.size() + " worker(s) alive found !");
		
		return hostAlive;
	}
	
	
	/**
	 * Test if a host is alive
	 * @param host
	 * @return true if it's alive
	 */
	public boolean isAlive(String host) {
		boolean alive = false;
		// test if this host is alive
		try {
			String cmd = "echo " + host;
			Shell shell = new SSH(host, shellPort, Constant.USERNAME, dsaKey);
			new Shell.Plain(shell).exec(cmd);
			alive = true;
		} catch (Exception e) {} // Fail to connect to the host
		return alive;
	}
	
	
	/**
	 * Return the cores number from the distant computer
	 * @param host
	 * @return cores
	 */
	public int getCoresNumber(String host) {
		int cores = 0;
		// test if this host is alive
		try {
			// get the number of cores
			String cmd = "grep -c ^processor /proc/cpuinfo";
			Shell shell = new SSH(host, shellPort, Constant.USERNAME, dsaKey);
			String stdout = new Shell.Plain(shell).exec(cmd);
			cores = Integer.parseInt(stdout.trim()); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cores;
	}
	
	
	/**
	 * Return list of hostname from a file
	 * @param random
	 * @return list hosts from the file
	 */
    public List<String> getHostFromFile(boolean random) {
    	List<String> hostnameMappers = new ArrayList<String>();

		// check first for this computer : the master is the worker
    	int cores = Runtime.getRuntime().availableProcessors();
		for (int i = 0; i < cores; i++) {
			hostnameMappers.add(hostFull);
		}
		
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
		 
		if (random) {
			Collections.shuffle(hostnameMappers);
		}
		 
    	return hostnameMappers;
    }
    
    
    /**
     * Return the dsa key
     * @param dsaFile
     * @return dsa key
     */
	public String getDsaKeyContent(String dsaFile) {
		String dsaKeyContent = null;	

		try {
			InputStream ips=new FileInputStream(dsaFile); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String line;
			while((line=br.readLine())!=null){
				dsaKeyContent += line + "\n";
			}
			br.close();
			if (Constant.MODE_DEBUG) System.out.println("Dsa key found");
		} catch (IOException e) {
			System.out.println("No dsa file");
		}

		return dsaKeyContent;
	}

	
	/**
	 * Get the network's ip adress
	 * @param regex
	 */
	public void generateNetworkIpAdress(String regex) {
		
		String cmdLine = "nmap -sn " + ipAdress + "/24 | awk \'{print $5}\' | grep -o " + regex;
		
		try {
			String line;
			// Run a java app in a separate system process
			String[] cmd = {
					"/bin/sh",
					"-c",
					cmdLine
			};
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			
			List<String> listIpAdress = new ArrayList<String>();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()) );
			while ((line = in.readLine()) != null) {
				listIpAdress.add(line);
			}
			in.close();
			
			if (Constant.MODE_DEBUG) System.out.println("On local : " + cmdLine);
			Util.writeFile(Constant.PATH_NETWORK_IP_DEFAULT_FILE, listIpAdress);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * Return true if the worker is the master
	 * @param worker
	 * @return true if it's the master
	 */
	public boolean isLocal(String worker) {
		boolean local = false;
		if (worker.equalsIgnoreCase(hostFull)) {
			// the worker is the master
			local = true;
		}
		return local;
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
		
}
