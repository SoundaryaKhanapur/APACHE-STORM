package com.git.smk.storm.sentiment;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

public class BrandRecognizingBolt extends BaseBasicBolt{
	
public static void main(String args[]) throws Exception  {
       
        SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
        String[] tokens = tokenizer.tokenize("i like google");
        String brand = Arrays.stream(tokens).filter(s->{return (s.equalsIgnoreCase("apple")||s.equalsIgnoreCase("google")||s.equalsIgnoreCase("windows"));}).findFirst().get();
        System.out.println(brand);
       /* InputStream inputStreamNameFinder = new FileInputStream("./src/main/resources/en-ner-organization.bin");
        TokenNameFinderModel model = new TokenNameFinderModel(inputStreamNameFinder);
        NameFinderME nameFinderME = new NameFinderME(model);
        List<Span> spans = Arrays.asList(nameFinderME.find(tokens));
        //assertThat(spans.toString()).isEqualTo("[[0..1) person, [13..14) person, [20..21) person]");
        List<String> names = new ArrayList<String>();
        int k = 0;
        for (Span s : spans) {
            names.add("");
            for (int index = s.getStart(); index < s.getEnd(); index++) {
                names.set(k, names.get(k) + tokens[index]);
            }
            k++;
        }
        System.out.println(names);*/
        //assertThat(names).contains("John","Leonard","Penny");
    }

@Override
public void execute(Tuple tuple, BasicOutputCollector collector) {
	String quote = tuple.getStringByField("quote");
	SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
    String[] tokens = tokenizer.tokenize(quote);
    try{
    String brand = Arrays.stream(tokens).filter(s->{return (s.equalsIgnoreCase("apple")||s.equalsIgnoreCase("google")||s.equalsIgnoreCase("microsoft"));}).findFirst().get();
	if(null!=brand||!"".equals(brand))
		collector.emit(new Values(tuple.getStringByField("sentiment"),tuple.getLongByField("timestamp"),brand));
    }catch(NoSuchElementException nse){
    	nse.printStackTrace();
    }
    
    
    	
    
    
}

@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {
	
	
	declarer.declare(new Fields("sentiment", "timestamp", "brand"));
	
	
	
}

}
