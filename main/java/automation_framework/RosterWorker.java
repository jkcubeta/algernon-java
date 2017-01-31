package automation_framework;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

class RosterWorker implements Runnable{
	@Override
	public void run(){
		Data credibleClients = null;
		try {
			credibleClients = new Data(Robot.keys.getKeyByName(Robot.internalKey,"algy_client_update_dump"), "", "");
		}catch(Exception e){
			e.printStackTrace();
		}
		Map<String,Map<String,String>> credibleClientData = credibleClients.getMapData();
		try {
			TreeMap<String,TreeMap<String,String>> algernonClientData = Robot.access.getAlgernonClients();
			Set<String> algyClientIds = new HashSet<>(algernonClientData.keySet());
			Set<String> credibleClientIds = new HashSet<>(credibleClientData.keySet());
			//check for new clients, add them if found
			if (!credibleClientIds.equals(algyClientIds)) {
				credibleClientIds.removeAll(algyClientIds);
				for (String credibleClientId : credibleClientIds) {
					Robot.access.addAlgernonClient(credibleClientData.get(credibleClientId));
				}
			}
			//check for changes in monitored client fields
            TreeMap<String,TreeMap<String,String>> allUpdateFields = new TreeMap<>();
			for(String algyClientId : algernonClientData.keySet()) {
				Map<String, String> algernonClient = algernonClientData.get(algyClientId);
				Map<String, String> credibleClient = credibleClientData.get(algyClientId);
				if (!algernonClient.equals(credibleClient)) {
					TreeMap<String, String> updateFields = new TreeMap<>();
                    for(String entry : credibleClient.keySet()){
                        String algyField = algernonClient.get(entry);
                        String credibleField = credibleClient.get(entry);
                        try {
                            if (!credibleField.equals(algyField)) {
                                updateFields.put(entry, credibleField);
                            }
                        }catch (NullPointerException e){
                            e.printStackTrace();
                        }
                    }
                    allUpdateFields.put(algyClientId,updateFields);
				}
			}
            for(String clientId : allUpdateFields.keySet()){
                try {
                    Robot.access.updateAlgernonClient(clientId, allUpdateFields.get(clientId));
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
			System.out.println("just did one cycle");
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
}