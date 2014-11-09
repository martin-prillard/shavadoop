package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;

public class Main {


    public static void main( String[] args )
    {
    	
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
