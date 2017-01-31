package automation_framework;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

class HtmlOutputRecord {
	private String tableHtml;
	private Integer credibleId;
	private Timestamp endTime;

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

}