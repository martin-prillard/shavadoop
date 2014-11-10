package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropReader;

public class Main {


    public static void main( String[] args )
    {
    	PropReader prop = new PropReader();
    	SSHManager sm = new SSHManager();
    	// get network's ip adress
    	if (prop.getPropValues(PropReader.FILE_IP_ADRESS) == null
    			|| prop.getPropValues(PropReader.FILE_IP_ADRESS).equals("")) {
    		if (Constant.APP_DEBUG) System.out.println(Constant.APP_DEBUG_TITLE + " Generate network's IP adress file " + Constant.APP_DEBUG_TITLE);
    		sm.generateNetworkIpAdress();
    	} else {
    		Constant.NETWORK_IP_FILE = prop.getPropValues(PropReader.FILE_IP_ADRESS);
    	}
    	
    	if (args.length == 0) {
    		// Launch the master
        	Master m = new Master();
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
