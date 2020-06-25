package idlab.massif.sinks.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;


@WebSocket
public class WebSocketOutputStream {

	private List<Session> sessions;
	public WebSocketOutputStream() {
		sessions = new ArrayList<Session>();
	}
    @OnWebSocketConnect
    public void connected(Session session) {
    	
    	System.out.println("connecting");
    	if(!sessions.contains(session)) {
    		sessions.add(session);  		
    	}
    }

    @OnWebSocketClose
    public void closed(Session session, int statusCode, String reason) {
    	System.out.println("closing");
    	if(sessions.contains(session)) {
    		sessions.remove(session);
    	}
    }

    @OnWebSocketMessage
    public void message(Session session, String message) throws IOException {

    }
    public void notify(String event) {
    	System.out.println("sending");
    	for(Session session: sessions) {
    		try {
    			
				session.getRemote().sendString(event);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    public void close() {
    	for(Session session: sessions) {
    		session.close();
    	}
    }

}
