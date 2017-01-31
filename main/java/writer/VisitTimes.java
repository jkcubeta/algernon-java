package writer;

import algernon.AlgyWhisperer;
import algernon.RemoteData;
import algernon.RemoteDatabaseConnectionError;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VisitTimes {
    private Long startTime;
    private Long endTime;
    private Long oneMinute = 60000L;

    VisitTimes(String plannerId, AlgyWhisperer whisperer){
        List<Time> times = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        RemoteData timeData = whisperer.getTxPlusTimeData(plannerId);
        for(Map<String,String> tableRow : timeData.getData()){
            String startTimeString = tableRow.get("start_time");
            String endTimeString = tableRow.get("end_time");
            try {
                Time startTime = new Time(sdf.parse(startTimeString).getTime());
                Time endTime = new Time(sdf.parse(endTimeString).getTime());
                //Timestamp startTime = new Timestamp(sdf.parse(startTimeString).getTime());
                //Timestamp endTime = new Timestamp(sdf.parse(endTimeString).getTime());
                times.add(startTime);
                times.add(endTime);
            }catch(ParseException e){
                throw new RuntimeException("cannot parse information for visit time calculations, "+e.getMessage());
            }
        }
        if(times.size()>2){
            throw new RemoteDatabaseConnectionError("error while attempting to retrieve the stored times for note submission, planner_id: "+plannerId);
        }
        this.startTime = times.get(0).getTime();
        this.endTime = times.get(1).getTime();
    }

    String getInterventionString(Intervention intervention){
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        StringBuilder builder = new StringBuilder();
        Long milDuration = (intervention.getDuration().longValue())*oneMinute;
        Long calcEndTime = startTime + milDuration;
        builder.append(sdf.format(new java.util.Date(startTime))).append(" - ").append(sdf.format(new java.util.Date(calcEndTime)));
        startTime = startTime+oneMinute;
        return builder.toString();
    }

}
