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
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			
			
			_clientSocket = _serverSocket.accept();
			DataInputStream is = new DataInputStream(_clientSocket.getInputStream());
		
//			if(is.available() != 0) {
				System.out.println("data is coming...");
				String word = is.readUTF();
				System.out.println(word);
				_collector.emit(new Values(word));
//			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("fail......");
		}
	}
		
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("proxy"));
	}
}
