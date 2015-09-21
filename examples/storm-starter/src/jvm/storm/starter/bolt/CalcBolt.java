package storm.starter.bolt;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class CalcBolt extends BaseBasicBolt{

	/********************************* init to R Server  */
	static String R_host = "163.180.117.72";
	static int R_port = 6311;
	static RConnection MASTER = RConnect(R_host, R_port);
	/*****************************************************/
	
	/*********************************************** init to DB Server  */
	static String m_ip = "163.180.117.72";
	static int m_port = 40000;
	MongoClient m_cli = null;
	
	DBCollection coll = null;
	
	// connect DB
	DB db = null;
	/********************************************************************/
	
	public int mongo(String ip, int port, String dbname) throws Exception{
		m_cli = new MongoClient(new ServerAddress(ip,port));
		db = m_cli.getDB(dbname);
		
		int a = 0;
		
		coll = db.getCollection("mean");
		DBCursor cur = coll.find();
		
		for(DBObject doc : cur) {
        	a = Integer.parseInt(doc.get("mean").toString());
        }
		
		BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("mean", 100);
        
        BasicDBObject searchQuery = new BasicDBObject().append("mean",140);
        
        coll.update(searchQuery, newDocument);
        
       return a;
	}
	
	// save mean value change to string
	static String s_mean = "";
	
	// save receive data
	static String r_data = "";
	
	// get mean value from DB
	static int mean = 0;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector){
		// TODO Auto-generated method stub
		try {
			
			mean = mongo(m_ip,m_port,"kocom_db");
			
			r_data = tuple.getString(0);
			s_mean = String.valueOf(mean);
			System.out.println("Receive data is...  " + r_data);
			
			REXP data = RQuery(MASTER, "mean(c(" + s_mean+ "," + r_data +"))");
			String[] result = data.asStrings();
			
			for(int i=0 ; i< result.length ; i++) {
				System.out.println(result[i]);
			}
			
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put("mean", Integer.getInteger(result[0]));
			BasicDBObject searchQuery = new BasicDBObject().append("mean", mean);
			
			coll.update(searchQuery, newDocument);
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
