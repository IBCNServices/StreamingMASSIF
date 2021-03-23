package idlab.massif.utils;

public class FormatUtils {
	
	public static String encodeQuery(String query) {
		return query.replace("\t", "\\t").replace("\n", "\\n").replace("\"", "\\\"");
	}

}
