package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;

public class CalcBolt extends BaseBasicBolt{

	static String HOST = "163.180.117.72";
	static int PORT = 6311;
	
	static RConnection MASTER = RConnect(HOST, PORT);
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector){
		// TODO Auto-generated method stub
		try {
			System.out.println("Receive data is...  " + tuple.getString(0));
			
			REXP data = RQuery(MASTER, "summary(c(12,354,354,654,321,684,315,351))");
			String[] result = data.asStrings();
			
			for(int i=0 ; i< result.length ; i++) {
				System.out.println(result[i]);
			}
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			System.out.println("error!!!!!!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		
	}
	
	// Connect R Master
	private static RConnection RConnect(String HOST, int PORT) {
		try {
			return new RConnection(HOST, PORT);
		} catch (RserveException e) {
			System.out.println("R Master Connect Fail..");
		}
		
		return null;
	}
	
	// Query to R Connection
	private static REXP RQuery(RConnection RC, String QUERY) {
		if(RC.isConnected()) {
			try {
				return RC.eval(QUERY);
			} catch (RserveException e) {
				// TODO Auto-generated catch block
				System.out.println("R Evaluation Fail");
			}
		}
		return null;
	}

}
