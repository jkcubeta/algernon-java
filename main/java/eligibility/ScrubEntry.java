package eligibility;

import java.sql.Timestamp;

class ScrubEntry {
    String clientId;
    Timestamp startTime;
    Timestamp endTime;

    ScrubEntry(String clientId){
        this.clientId = clientId;
        this.startTime = new Timestamp(new java.util.Date().getTime());
    }

}