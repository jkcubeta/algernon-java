package eligibility;

import java.util.Map;

class InputRecord {
	private Map<String,String> record;

	InputRecord(Map<String,String> input){
        this.record = input;
    }

	String getField(String type){
		return record.get(type);
	}

}