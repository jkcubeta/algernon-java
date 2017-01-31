package automation_framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

class InputRecord {
	private Map<String,String> record;

	private InputRecord(ArrayList<String> credibleRecord) {
		record = new TreeMap<>();
		ArrayList<String> fields = new ArrayList<>(Arrays.asList("credibleId", "medicaidId"));
		int q = 0;
		for (String entry : credibleRecord) {
			record.put(fields.get(q), entry);
		}
	}

	private InputRecord(){
		record = new TreeMap<>();
        String medicaidId;
        String targetId;
		try {
			targetId = Robot.access.getOldestId();
            try {
                Data medicaidData = new Data(Robot.algyWhisperer.getEncryptedKeyByName("algy_medicaid_pull",Robot.internalKey,Robot.keys), targetId, "");
                medicaidId = medicaidData.getMapData().get(targetId).get("medicaid_number");
            }catch(Exception e){
                System.out.println("can't reach off site database, using local value for "+targetId);
                medicaidId = Robot.access.getMedicaidId(targetId);
            }

			record.put("credible_id",targetId);
			record.put("medicaid_id",medicaidId);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	InputRecord(String credibleId, String medicaidId){
		record = new TreeMap<>();
		record.put("credible_id",credibleId);
		record.put("medicaid_id",medicaidId);
	}

	InputRecord(Map<String,String> input){
        this.record = input;
    }

	String getField(String type){
		return record.get(type);
	}

}