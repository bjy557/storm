package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CalcBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("my name is.............." + tuple.getString(0) + "end....................");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		
	}

}
