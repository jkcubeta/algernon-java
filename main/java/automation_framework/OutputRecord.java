package automation_framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

class OutputRecord{
	private LinkedHashMap<String,String> outputRecord;
	private ArrayList<String> fields = new ArrayList<>(Arrays.asList(
			"medicaid_name_check",
			"medicaid_gender_check",
			"medicaid_dob_check",
			"medicaid_status",
			"medicaid_start_date",
			"medicaid_end_date",
            "qmb_code",
			"medicare_status",
			"medicare_start_date",
			"medicare_end_date",
			"mco_status",
			"mco_start_date",
			"mco_end_date",
			"tpl_name"));
	private String credibleId;

	OutputRecord(String credibleId, ArrayList<String> profileData){
		outputRecord = new LinkedHashMap<>();
		this.credibleId = credibleId;
		int q = 0;
		for(String data : profileData){
			outputRecord.put(fields.get(q),data);
			q++;
		}
	}

	OutputRecord(RecordPart recordPart){
		this.credibleId = recordPart.getCredibleId();
		this.outputRecord = recordPart.getMap();
	}

	OutputRecord(String credibleId){
		outputRecord = new LinkedHashMap<>();
		this.credibleId = credibleId;
		for (String field : fields){
			outputRecord.put(field,null);
		}
	}

	String getCredibleId() {
		return credibleId;
	}

	String getField(String fieldName){
		return outputRecord.get(fieldName);
	}

	LinkedHashMap<String,String> getFields() {
		return outputRecord;
	}

}