package fr.telecompt.shavadoop.main;

import java.io.File;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;
import fr.telecompt.shavadoop.util.Util;

public class Main {
		
    public static void main( String[] args )
    {
    	
    	SSHManager sm = new SSHManager();
    	sm.initialize();
    	
    	if (args.length == 0) {
    		
    		if(!Constant.MODE_SCP_FILES) {
    			// clean res directory
    			Util.initializeResDirectory(Constant.PATH_REPO_RES, true);
    		}
    		
    		// get network's ip adress
        	PropReader prop = new PropReader();
        	String ipFileString = prop.getPropValues(PropReader.FILE_IP_ADRESS);
        	File ipFile = new File(ipFileString);
        	// if no ip file given
        	if (!ipFile.exists()) {
        		if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Generate network's IP adress file " + Constant.APP_DEBUG_TITLE);
        		sm.generateNetworkIpAdress(prop.getPropValues(PropReader.NETWORK_IP_REGEX));
        	} else {
        		Constant.PATH_NETWORK_IP_FILE = ipFileString;
        	}
        	
    		// Launch the master
        	Master m = new Master(sm);
        	m.initialize();
        	m.launchMapReduce();
        	
    	} else if (args.length == 5){
    		// Launch the slave
    		Slave s = new Slave(sm, args[0], args[1], args[2], args[3], args[4]);
    		s.launchWork();
    	} else {
    		System.out.println("Not enough args");
    	}
    	
    }
    
}
