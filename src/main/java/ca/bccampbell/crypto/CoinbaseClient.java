package ca.bccampbell.crypto;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.util.concurrent.Future;
import java.io.*;
import org.apache.commons.io.IOUtils;

import java.util.concurrent.TimeUnit;

public class CoinbaseClient {
	 private PipedWriter writer;// = new PipedWriter();
    private PipedReader reader;//  = new PipedReader(wsClientWriter);
  
    public CoinbaseClient() throws IOException {
       writer = new PipedWriter();
       reader = new PipedReader(writer);
    }
    
    public PipedReader getReader() {
   	 return reader;
    }
    
    public static void main (String[] args) throws IOException {
   	 System.out.println(System.getProperty("java.class.path"));
   	 CoinbaseClient sc = new CoinbaseClient();
       sc.runCoinbase();
       //jwt.runCoinbase();
    }

    public void runCoinbase() throws IOException {
       // Writing data to pipe
     	 WebSocketClient client = new WebSocketClient();
       Thread writerThread = new Thread(new Runnable() {
          @Override
          public void run() {
 	           try {
	              URI uri = new URI("wss://ws-feed.pro.coinbase.com");    	
	           	  client.start();
	              EventWebSocket socket = new EventWebSocket(writer);
	              Future<Session> fut = client.connect(socket, uri);
	              Session session = fut.get();
	              session.getRemote().sendString("{\"type\": \"subscribe\", \"product_ids\": [\"BTC-USD\",\"ETH-USD\",\"XRP-USD\",\"LTC-USD\",\"BCH-USD\"], \"channels\": [\"heartbeat\", {\"name\": \"ticker\", \"product_ids\": [\"BTC-USD\",\"ETH-USD\",\"XRP-USD\",\"LTC-USD\",\"BCH-USD\"]}]}");
	        		
	              while (!Thread.currentThread().isInterrupted()) {
	            	  writer.flush();
	              }
	              session.close();
	              client.stop();
	              socket.writer.close();            	 
             } catch (Exception e) {
                e.printStackTrace();
             }
          }
       });
       writerThread.start();
    }
   
	 public class EventWebSocket extends WebSocketAdapter {
		PipedWriter writer = new PipedWriter();
		
		public EventWebSocket(PipedWriter writer) {
			this.writer = writer;
		}
	    @Override
	    public void onWebSocketConnect(Session sess) {
	      super.onWebSocketConnect(sess);
	    }
	    @Override
	    public void onWebSocketText(String message) {
	        super.onWebSocketText(message);
   		  try {
                writer.write(message + "\n");
   		  } catch (Exception e) {
   	         e.printStackTrace();
   		  }
	    }
	    @Override
	    public void onWebSocketClose(int statusCode, String reason) {
	      super.onWebSocketClose(statusCode, reason);
	    }
	    @Override
	    public void onWebSocketError(Throwable cause) {
	      super.onWebSocketError(cause);
	      cause.printStackTrace(System.err);
	    }
	}
}
