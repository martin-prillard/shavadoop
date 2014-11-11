package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;

public class Main {


//	Kill all java processus : //TODO remove
//	ps -ef | grep java|cut -b10-15 | xargs kill -9
		
    public static void main( String[] args )
    {
    	
    	if (args.length == 0) {
    		// get network's ip adress
        	PropReader prop = new PropReader();
        	String ipFile = prop.getPropValues(PropReader.FILE_IP_ADRESS);
        	// if no ip file given
        	if (ipFile == null || ipFile.equals("")) {
        		if (Constant.MODE_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Generate network's IP adress file " + Constant.APP_DEBUG_TITLE);
        		SSHManager.generateNetworkIpAdress(prop.getPropValues(PropReader.NETWORK_IP_REGEX));
        	} else {
        		Constant.NETWORK_IP_FILE = ipFile;
        	}
        	
    		// Launch the master
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
        	
    	} else if (args.length == 3){
    		// Launch the slave
    		Slave s = new Slave(args[0], args[1], args[2]);
    		s.launchWork();
    	} else {
    		System.out.println("Not enough args");
    	}
    	
    }
    
}
