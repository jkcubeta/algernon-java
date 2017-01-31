package writer;

import algernon.*;
import org.openqa.selenium.Keys;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoteInformation {
    private String clientId;
    private String presentation;
    private String goals;
    private String objectives;
    private String interventions;
    private String response;

    public NoteInformation(String plannerId, String clientId, AlgyRobotWhisperer robotWhisperer, AlgyWhisperer whisperer){
        try {
            this.clientId = clientId;
            presentation = robotWhisperer.getRawPresentation(plannerId);
            if( presentation == null || presentation.isEmpty()){
                presentation = "!!!!! no presentation entered !!!!!";
            }
            List<Intervention> rawInterventions = robotWhisperer.getRawInterventions(plannerId);
            VisitTimes times = new VisitTimes(plannerId,whisperer);
            StringBuilder goalBuilder = new StringBuilder();
            StringBuilder objectiveBuilder = new StringBuilder();
            StringBuilder interventionBuilder = new StringBuilder();
            StringBuilder responseBuilder = new StringBuilder();
            Integer q = 1;
            for (Intervention intervention : rawInterventions) {
                String timeString = times.getInterventionString(intervention);
                Map<String,String> parents = getRawParents(intervention,whisperer);
                goalBuilder.append(q).append(". ").append(parents.get("goal")).append(Keys.RETURN);
                objectiveBuilder.append(q).append(". ").append(parents.get("objective")).append(Keys.RETURN);
                interventionBuilder.append(q).append(".").append("(").append(timeString).append(")   ").append(intervention.getDescription()).append(Keys.RETURN);
                String interventionResponse = intervention.getResponse();
                responseBuilder.append(q).append(". ").append(interventionResponse).append(Keys.RETURN);
                q++;
            }
            goals = goalBuilder.toString();
            objectives = objectiveBuilder.toString();
            interventions = interventionBuilder.toString();
            response = responseBuilder.toString();
        }catch(SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }catch(Exception e){
            throw new RemoteDatabaseConnectionError();
        }


    }

    synchronized String getGoals(){
        return goals;
    }

    synchronized String getObjectives(){
        return objectives;
    }

    synchronized String getInterventions(){
        return interventions;
    }

    private Map<String,String> getRawParents(Intervention intervention, AlgyWhisperer whisperer) throws SQLException{
        Map<String,String> parentElements = new HashMap<>();
        try {
            RemoteData parentData = whisperer.getRawTxPlusParents(intervention);
            for(Map<String,String> tableRow : parentData.getData()){
                String goal = tableRow.get("goal");
                String objective = tableRow.get("objective");
                parentElements.put("goal",goal);
                parentElements.put("objective",objective);
            }
        }catch(Exception e){
            throw new RemoteDatabaseConnectionError();
        }
        return parentElements;
    }

    String getClientId() {
        return clientId;
    }

    String getPresentation() {
        return presentation;
    }

    String getResponse() {
        return response;
    }
}
