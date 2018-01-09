package org.example.abd;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.example.abd.cmd.Command;
import org.example.abd.cmd.CommandFactory;
import org.example.abd.cmd.ReadReply;
import org.example.abd.cmd.ReadRequest;
import org.example.abd.cmd.WriteReply;
import org.example.abd.cmd.WriteRequest;
import org.example.abd.quorum.Majority;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class RegisterImpl<V> extends ReceiverAdapter implements Register<V>{

    private String name;
    private V value;
    private int label;
    private int max;
    private boolean isWritable;
    private Majority quorumSystem;
    
    private CommandFactory<V> factory;
    private JChannel channel;

    private CompletableFuture<Pair<V,Integer>> pending;
    private List<Command<V>> replies;

    public RegisterImpl(String name) {
        this.name = name;
        this.factory = new CommandFactory<>();
    }

    public void init(boolean isWritable) throws Exception {
    	value = null;
    	label = 0;
    	max = 0;
    	this.isWritable = isWritable;
    	
    	channel = new JChannel();
    	channel.setReceiver(this);
    	channel.connect(name);
    }

    @Override
    public void viewAccepted(View view) {
    	quorumSystem = new Majority(view);
    }

    // Client part

    @Override
    public V read() {
        Pair<V,Integer> pair = execute(factory.newReadRequest());
        // read-repair:
    	execute(factory.newWriteRequest(pair.getLeft(), pair.getRight()));
        return pair.getLeft();
    }

    @Override
    public void write(V v) throws IllegalStateException {
    	if (!isWritable) {
    		throw new IllegalStateException("The register isn't writable");
    	}
    	execute(factory.newWriteRequest(v, ++max));
    }

    private synchronized Pair<V,Integer> execute(Command cmd) {
		pending = new CompletableFuture<>();
		replies = new CopyOnWriteArrayList<>();
		for (Address address : quorumSystem.pickQuorum()) {
			send(address, cmd);
		}
        return pending.join();
    }

    // Message handlers

    @Override
    public void receive(Message msg) {
    	Address sender = msg.getSrc();
    	Command<V> cmd = (Command<V>) msg.getObject();
    	if (cmd instanceof ReadRequest) {
    		send(sender, factory.newReadReply(value, label));
    	}
    	else if (cmd instanceof ReadReply) {
    		replies.add(cmd);
    		if (replies.size() == quorumSystem.quorumSize()) {
    			V replyValue = replies.get(0).getValue();
    			int maxLabel = replies.get(0).getTag();
    			for (Command<V> reply : replies) {
    				if (reply.getTag() > maxLabel) {
    					maxLabel = reply.getTag();
    					replyValue = reply.getValue();
    				}
    			}
    			pending.complete(Pair.of(replyValue, maxLabel));
    		}
    	}
    	else if (cmd instanceof WriteRequest) {
    		if (cmd.getTag() > label) {
    			value = cmd.getValue();
    			label = cmd.getTag();
    		}
    		send(sender, factory.newWriteReply());
    	}
    	else if (cmd instanceof WriteReply) {
    		replies.add(cmd);
    		if (replies.size() == quorumSystem.quorumSize()) {
    			pending.complete(null);
    		}
    	}
    }

    private void send(Address dst, Command command) {
        try {
            Message message = new Message(dst, channel.getAddress(), command);
            channel.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void close() {
    	channel.close();
    }
}
