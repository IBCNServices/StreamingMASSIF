package idlab.massif.timeout;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import idlab.massif.interfaces.core.ListenerInf;

public class KeyedTimeoutTest {
	
	@Test
	public void singleKeyTest() {
		long timeOut = 1000;
		KeyedTimeout ktimeout = new KeyedTimeout(timeOut+"", "Select ?key WHERE{?key a <http://test/Event>}");
		TestListener listener = new TestListener();
		ktimeout.addListener(listener);
		String event1 = "<http://test/event1> a <http://test/Event>";
		ktimeout.addEvent(event1);
		Assert.assertEquals(listener.events.size(), 1);
		Assert.assertEquals(event1, listener.events.get(0));
		//we send same event directly after, which should not be forwarded
		ktimeout.addEvent(event1);
		Assert.assertEquals(listener.events.size(), 1);
	}
	@Test
	public void waitKeyTest() {
		long timeOut = 1000;
		KeyedTimeout ktimeout = new KeyedTimeout(timeOut+"", "Select ?key WHERE{?key a <http://test/Event>}");
		TestListener listener = new TestListener();
		ktimeout.addListener(listener);
		String event1 = "<http://test/event1> a <http://test/Event>";
		ktimeout.addEvent(event1);
		Assert.assertEquals(listener.events.size(), 1);
		Assert.assertEquals(event1, listener.events.get(0));
		try {
			Thread.sleep(2*timeOut);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//we send another event directly after, which should not be forwarded
		ktimeout.addEvent(event1);
		Assert.assertEquals(listener.events.size(), 2);
	}
	@Test
	public void multipleKeyTest() {
		KeyedTimeout ktimeout = new KeyedTimeout("1000", "Select ?key WHERE{?key a <http://test/Event>}");
		TestListener listener = new TestListener();
		ktimeout.addListener(listener);
		String event1 = "<http://test/event1> a <http://test/Event>";
		ktimeout.addEvent(event1);
		Assert.assertEquals(listener.events.size(), 1);
		Assert.assertEquals(event1, listener.events.get(0));
		//we send another event directly after, which should  be forwarded
		String event2 = "<http://test/event2> a <http://test/Event>";
		ktimeout.addEvent(event2);
		Assert.assertEquals(listener.events.size(), 2);
		Assert.assertEquals(event2, listener.events.get(1));

	}
	
	
	private class TestListener implements ListenerInf{
		
		public List<String> events = new ArrayList<String>();

		@Override
		public void notify(int queryID, String event) {
			this.events.add(event);
			
		}
		
	}

}
