package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropertiesReader;

public class Main {


    public static void main( String[] args )
    {
    	
    	if (args.length == 0) {
    		// Launch the master
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
    	} else if (args.length == 4){
    		// Launch the slave
    		new Slave(args[0], args[1], args[2], args[3]);
    	} else {
    		System.out.println("Not enough args");
    	}
    	
    }
    
}
