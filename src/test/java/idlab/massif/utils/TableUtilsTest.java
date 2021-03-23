package idlab.massif.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import junit.framework.Assert;

public class TableUtilsTest {
	
	@Test
	public void singleKeyResultTableTest() {
		String queryResult = "key,\n"
				+ "http://test/event1,";
		Map<String,List<String>> result  = TableUtils.convertStringToTable(queryResult);
		Map<String,List<String>> expected   = new HashMap<String,List<String>>();
		expected.put("key", new ArrayList<String>());
		expected.get("key").add("http://test/event1");		
		Assert.assertEquals(expected, result);

	}
	@Test
	public void whiteSpaceTableTest() {
		String queryResult = "key , \n"
				+ "http://test/event1 , ";
		Map<String,List<String>> result  = TableUtils.convertStringToTable(queryResult);
		Map<String,List<String>> expected   = new HashMap<String,List<String>>();
		expected.put("key", new ArrayList<String>());
		expected.get("key").add("http://test/event1");		
		Assert.assertEquals(expected, result);

	}
	@Test
	public void multipleLinesTableTest() {
		String queryResult = "key , \n"
				+ "http://test/event3 , \n"
				+ "http://test/event2 , \n"
				+ "http://test/event1 , ";
		Map<String,List<String>> result  = TableUtils.convertStringToTable(queryResult);
		Map<String,List<String>> expected   = new HashMap<String,List<String>>();
		expected.put("key", new ArrayList<String>());
		expected.get("key").add("http://test/event3");		
		expected.get("key").add("http://test/event2");
		expected.get("key").add("http://test/event1");
		Assert.assertEquals(expected, result);

	}
	@Test
	public void multipleKeysTableTest() {
		String queryResult = "key1, key2\n"
				
				+ "http://test/event1 , http://test/event2";
		Map<String,List<String>> result  = TableUtils.convertStringToTable(queryResult);
		Map<String,List<String>> expected   = new HashMap<String,List<String>>();
		expected.put("key1", new ArrayList<String>());
		expected.put("key2", new ArrayList<String>());
		expected.get("key2").add("http://test/event2");
		expected.get("key1").add("http://test/event1");
		Assert.assertEquals(expected, result);

	}

}
