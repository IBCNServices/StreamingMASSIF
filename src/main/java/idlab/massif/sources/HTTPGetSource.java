package idlab.massif.sources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import idlab.massif.core.PipeLine;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;

/***
 * Allows to continuously pull a HTTP source through a HTTP get request.
 * 
 * @author psbonte
 *
 */
public class HTTPGetSource implements SourceInf {

	private String url;
	private PipeLine pipeline;
	private long pullTimeOut;
	private ListenerInf listener;
	private boolean streaming = true;

	public HTTPGetSource(String url, long pullTimeOut) {
		this.url = url;
		this.pullTimeOut = pullTimeOut;
	}

	public void registerPipeline(PipeLine pipeline) {
		this.pipeline = pipeline;
	}

	public void stream() {
		while (streaming) {
			URL urlCon;
			try {
				urlCon = new URL(url);
				URLConnection conn = urlCon.openConnection();
				InputStream is = conn.getInputStream();
				String result = "";
				try (BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset()))) {
					result = br.lines().collect(Collectors.joining(System.lineSeparator()));
					JSONArray jsonRes = new JSONArray(result);
					for (int i = 0; i < jsonRes.length(); i++) {

						if (pipeline != null) {
							pipeline.addEvent(jsonRes.getString(i));
						}
						if (listener != null) {
							listener.notify(0, jsonRes.getString(i));
						}
					}
				}
				Thread.sleep(pullTimeOut);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	@Override
	public boolean addEvent(String event) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		this.listener = listener;
		return true;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		streaming = true;
		new Thread(() -> this.stream()).start();
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		streaming = false;
	}
}
