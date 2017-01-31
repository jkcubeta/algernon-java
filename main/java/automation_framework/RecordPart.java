package automation_framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

class RecordPart {

	private String credibleId;
	private LinkedHashMap<String,String> recordPart;

	RecordPart(String type){
		LinkedHashMap<String,ArrayList<String>> names = new LinkedHashMap<>();
		names.put("demographic", new ArrayList<>(Arrays.asList("medicaid_name", "medicaid_dob", "medicaid_gender")));
		names.put("medicaid", new ArrayList<>(Arrays.asList("medicaid_check", "medicaid_start_date", "medicaid_end_date", "qmb_code")));
		names.put("medicare", new ArrayList<>(Arrays.asList("medicare_check", "medicare_start_date", "medicare_end_date")));
		names.put("mco", new ArrayList<>(Arrays.asList("mco_name", "mco_start_date", "mco_end_date")));
		names.put("tpl", new ArrayList<>(Collections.singletonList("tpl_name")));

		recordPart = new LinkedHashMap<>();
		if(type.equals("all")){
			for(String key : names.keySet()){
				for(String subKey : names.get(key)) {
					recordPart.put(subKey,null);
				}
			}
		}else {
			for (String name : names.get(type)) {
				recordPart.put(name, null);
			}
		}
	}

	void setCredibleId(String credibleId){
		this.credibleId = credibleId;
	}

	String getCredibleId(){
	    return credibleId;
    }
	public LinkedHashMap<String,String> getMap(){
		return recordPart;
	}

	public String getValue(String key){
		return recordPart.get(key);
	}

	void setField(String key, String value){
		recordPart.put(key,value);
	}
}