package idlab.massif.sinks;

import java.net.URI;

import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SinkInf;
import idlab.massif.sinks.utils.WebSocketOutputStream;
import idlab.massif.sources.util.WebSocketInputStream;

public class WebSocketClientSink implements SinkInf {

	private String wsURL;
	private WebSocketOutputStream socket;
	public WebSocketClientSink(String wsURL) {
		this.wsURL = wsURL;
		WebSocketClient client = new WebSocketClient();

		this.socket = new WebSocketOutputStream();
		try {
			client.start();

			URI echoUri = new URI(wsURL);
			ClientUpgradeRequest request = new ClientUpgradeRequest();
			client.connect(socket, echoUri, request);
			System.out.printf("Connecting to : %s%n", echoUri);

		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			try {
				//client.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public boolean addEvent(String result) {
		// TODO Auto-generated method stub
		socket.notify(result);
		return true;
	}
	@Override
	public boolean addListener(ListenerInf listener) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		this.socket.close();
	}

}
