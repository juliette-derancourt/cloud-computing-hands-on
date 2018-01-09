package org.example.abd.quorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.View;

public class Majority {

	List<Address> processes = new ArrayList<>();
	int size;
	
    public Majority(View view) {
    	for (Address process : view.getMembers()) {
    		processes.add(process);
    	}
    	size = processes.size()/2 + 1;
    }

    public int quorumSize() {
        return size;
    }

    public List<Address> pickQuorum() {
    	Collections.shuffle(processes);
        return processes.subList(0, size);
    }

}
