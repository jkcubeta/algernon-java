package writer;

public class Intervention {
    private Integer interventionId;
    private Integer duration;
    private String description;
    private String response;

    public Intervention(Integer interventionId, Integer duration, String description, String response){
        this.interventionId = interventionId;
        this.duration = duration;
        this.description = description;
        this.response = response;
        if(this.response == null || this.response.isEmpty()){
            this.response = "!!!!! no response entered !!!!!";
        }
    }

    Integer getDuration(){
        return duration;
    }

    String getDescription(){
        return description;
    }

    String getResponse(){return response;}

    public synchronized Integer getInterventionId(){
        return interventionId;
    }
}
