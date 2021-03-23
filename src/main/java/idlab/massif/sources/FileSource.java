package idlab.massif.sources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import idlab.massif.core.PipeLine;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;

/***
 * Reads a provided file line by line and streams each line to the pipeline. If
 * timeout equals -1, it read the whole file in one go and then streams the
 * result.
 * 
 * @author psbonte
 *
 */
public class FileSource implements SourceInf {

	private String fileName;
	private PipeLine pipeline;
	private long timeout;
	private ListenerInf listener;
	private boolean streaming = true;

	public FileSource(String fileName, long timeout) {
		this.fileName = fileName;
		this.timeout = timeout;
	}

	public void registerPipeline(PipeLine pipeline) {
		this.pipeline = pipeline;
	}

	public void stream() {

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String line;
			if (timeout >= 0) {
				
				while ((line = br.readLine()) != null && streaming) {

					line += "\n";
					if (listener != null) {
						listener.notify(0, line);
					}
					if (pipeline != null) {
						pipeline.addEvent(line);
					}
					try {
						Thread.sleep(timeout);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			} else {
				//read the file as one and then stream the results
				StringBuilder result = new StringBuilder();
				while ((line = br.readLine()) != null && streaming) {
					result.append(line).append("\n");
				}
				if (listener != null) {
					listener.notify(0, result.toString());
				}
				if (pipeline != null) {
					pipeline.addEvent(result.toString());
				}
				
			}

		} catch (IOException e) {
			e.printStackTrace();
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

	@Override
	public String toString() {
		return String.format("{\"type\":\"Source\",\"impl\":\"fileSource\",\"fileName\":\"%s\",\"timeout\":%d}",
				fileName, timeout);
	}

}
