package fr.telecompt.shavadoop;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;

/**
 * 
 * @author martin prillard
 *
 */
public class Main {
		
    public static void main( String[] args )
    {
    	
    	if (args.length == 0) {
    		// Launch the master
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
        	
    	} else if (args.length == 5){
    		// Launch the slave
    		Slave s = new Slave(args[0], args[1], args[2], args[3], args[4]);
    		s.launchTask();
    		
    	} else {
    		System.out.println("Not enough args");
    	}
    	
    }
    
}
