package storm.starter;

import storm.starter.spout.ProxySpout;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.CalcBolt;

public class AverageTopology {
	
	private static final String PROXY_SPOUT_ID = "proxy-spout";
	private static final String CALC_BOLT_ID = "calc-bolt";

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		ProxySpout spout = new ProxySpout(31000);
		CalcBolt bolt = new CalcBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(PROXY_SPOUT_ID, spout);
		builder.setBolt(CALC_BOLT_ID, bolt).globalGrouping(PROXY_SPOUT_ID);
		
		Config conf = new Config();
		conf.setDebug(false);
		
		StormRunner.runTopologyRemotely(builder.createTopology(), "test", conf);
	}

}
