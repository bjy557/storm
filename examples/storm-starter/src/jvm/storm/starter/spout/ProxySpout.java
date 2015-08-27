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
//			System.out.println("tuple.................");
			InputStream is = _clientSocket.getInputStream();
//			System.out.println("data is.........." + is.toString());
			
			int i;
			StringBuffer buf = new StringBuffer();
			byte[] b = new byte[4096];
			while( (i = is.read(b)) != -1) {
				buf.append(new String(b, 0, i));
			}
			
			String word = buf.toString();
			
			System.out.println("word is.................. " +word +" end.....");
			
			_collector.emit(new Values(word));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("proxy"));
	}
}
