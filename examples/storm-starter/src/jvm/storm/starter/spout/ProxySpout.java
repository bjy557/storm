package storm.starter.spout;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import java.io.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ProxySpout extends BaseRichSpout{

	static SpoutOutputCollector _collector;
	
	static Socket _clientSocket;
	static ServerSocket _serverSocket;
	static int _port;
	
	public ProxySpout(int port){
		_port = port;
		System.out.println("port : " + _port);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		try {
			_serverSocket = new ServerSocket(31000);
			
			System.out.println("start listen.... " + _serverSocket.getLocalPort() + " / " + _serverSocket.isBound());
			_clientSocket = _serverSocket.accept();
			System.out.println("accept......................");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			String word;
			System.out.println("tuple.................");
			InputStream incomingIS = _clientSocket.getInputStream();
			System.out.println("data is.........." + incomingIS.toString());
			word = getStringFromInputStream(incomingIS);
			_collector.emit(new Values(word));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// convert InputStream to String
	public String getStringFromInputStream(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return sb.toString();

	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("proxy"));
	}
}
