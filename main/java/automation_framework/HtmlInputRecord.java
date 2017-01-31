package automation_framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

class HtmlInputRecord {
	private Map<String,String> record;

	private HtmlInputRecord(ArrayList<String> credibleRecord) {
		record = new TreeMap<>();
		ArrayList<String> fields = new ArrayList<>(Arrays.asList("credibleId", "medicaidId"));
		int q = 0;
		for (String entry : credibleRecord) {
			record.put(fields.get(q), entry);
		}
	}

	HtmlInputRecord(){
		record = new TreeMap<>();
        String medicaidId;
        String targetId;
		try {
            String[] oldestRecord = Robot.access.getOldestHtmlRecord();
			record.put("credible_id",oldestRecord[0]);
			record.put("medicaid_id",oldestRecord[1]);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	HtmlInputRecord(String credibleId, String medicaidId){
		record = new TreeMap<>();
		record.put("credible_id",credibleId);
		record.put("medicaid_id",medicaidId);
	}

	HtmlInputRecord(Map<String,String> input){
        this.record = input;
    }

	String getField(String type){
		return record.get(type);
	}

}