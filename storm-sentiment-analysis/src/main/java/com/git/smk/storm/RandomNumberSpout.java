package com.git.smk.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomNumberSpout extends BaseRichSpout {
	
    private Random random;
    private SpoutOutputCollector collector;
    
    
	public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
		random = new Random();
		collector=spoutOutputCollector;
		
	}
	public void nextTuple() {
		Utils.sleep(1000);
        //This will select random int from the range (0, 100)
        int operation = random.nextInt(101);
        long timestamp = System.currentTimeMillis();

        Values values = new Values(operation, timestamp);
        collector.emit(values);
		
	}
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		 outputFieldsDeclarer.declare(new Fields("operation", "timestamp"));
		
	}

}
