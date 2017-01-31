package eligibility;

import java.sql.Timestamp;

class HtmlOutputRecord {
	private String tableHtml;
	private Integer credibleId;
	private Timestamp endTime;
	private Timestamp startTime;

	HtmlOutputRecord(String credibleId, String tableHtml){
		this.credibleId = Integer.parseInt(credibleId);
		this.tableHtml = tableHtml;
	}

	HtmlOutputRecord(String credibleId){
		this.credibleId = Integer.parseInt(credibleId);
		this.tableHtml = null;
	}

	Integer getCredibleId() {
		return credibleId;
	}

	String getTableHtml(){
		return tableHtml;
	}

	void setEndTime(Timestamp endTime){
        this.endTime = endTime;
    }

    void setStartTime(Timestamp startTime){
		this.startTime = startTime;
	}

	Timestamp getStartTime(){
        return startTime;
    }

    Timestamp getEndTime(){
	    return endTime;
    }

}