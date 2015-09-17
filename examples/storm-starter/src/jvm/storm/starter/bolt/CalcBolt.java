package storm.starter.bolt;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class CalcBolt extends BaseBasicBolt{

	static String HOST = "163.180.117.72";
	static int PORT = 6311;
	
	static int mean = 0;
	
	static String s_mean = "";
	static String temp = "";
	
	static RConnection MASTER = RConnect(HOST, PORT);
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector){
		// TODO Auto-generated method stub
		try {
			temp = tuple.getString(0);
			s_mean = String.valueOf(mean);
			System.out.println("Receive data is...  " + temp);
			
			REXP data = RQuery(MASTER, "mean(c(" + s_mean + temp +"))");
			String[] result = data.asStrings();
			
			for(int i=0 ; i< result.length ; i++) {
				System.out.println(result[i]);
			}
		} catch (Exception e) {
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
		} catch (Exception e) {
			System.out.println("R Master Connect Fail..");
		}
		
		return null;
	}
	
	// Query to R Connection
	private static REXP RQuery(RConnection RC, String QUERY) {
		if(RC.isConnected()) {
			try {
				return RC.eval(QUERY);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("R Evaluation Fail");
			}
		}
		return null;
	}

}
