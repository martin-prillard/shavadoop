package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;

public class Main {

    public static void main( String[] args )
    {
    	if (args.length == 0) {
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
    	} else if (args.length == 3){
    		new Slave(args[0], args[1], args[2]);
    	} else {
    		System.out.println("Not enough args");
    	}

    }
    
    
}
