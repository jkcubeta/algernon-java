//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package inspector;

import java.util.ArrayList;
import java.util.Collections;

class InsuranceResults {
    Boolean uninsured = Boolean.FALSE;
    Boolean medicaid = Boolean.FALSE;
    Boolean mco = Boolean.FALSE;
    Boolean medicare = Boolean.FALSE;
    Boolean ltc = Boolean.FALSE;
    Boolean other = Boolean.TRUE;
    String program;
    String programCode;
    String mcoName;
    String status;
    String beginDate;
    String endDate;
    String qmbCode;
    String[] parts;

    InsuranceResults(String rawResults) {
        String shortenedResults = this.removeServices(rawResults);
        this.parts = this.standardize(shortenedResults);
        if(this.parts.length > 3 && this.parts.length < 20) {
            if(this.parts.length == 4) {
                if(this.parts[1].contains("N/A") && this.parts[2].contains("N/A") && this.parts[3].contains("N/A")) {
                    this.uninsured = Boolean.TRUE;
                    this.other = Boolean.FALSE;
                }
            } else if(this.parts.length == 7) {
                if(this.parts[4].contains("A HIC Number:")) {
                    this.medicare = Boolean.TRUE;
                    this.other = Boolean.FALSE;
                } else {
                    this.other = Boolean.TRUE;
                }
            } else {
                String program;
                String programCode;
                String status;
                String beginDate;
                String endDate;
                String qmbCode;
                if(this.parts.length == 9) {
                    if(this.parts[7].contains("N/A") && this.parts[8].contains("N/A")) {
                        program = this.parts[1].replace(String.valueOf(' '), "");
                        programCode = this.parts[2].replace(String.valueOf(' '), "");
                        status = this.parts[3].replace(String.valueOf(' '), "");
                        beginDate = this.parts[4].replace(String.valueOf(' '), "");
                        endDate = this.parts[5].replace(String.valueOf(' '), "");
                        qmbCode = this.parts[6].replace(String.valueOf(' '), "");
                        this.program = program.trim();
                        this.programCode = programCode.trim();
                        this.status = status.trim();
                        this.beginDate = beginDate.trim();
                        this.endDate = endDate.trim();
                        this.qmbCode = qmbCode.trim();
                        this.uninsured = Boolean.FALSE;
                        this.other = Boolean.FALSE;
                    } else {
                        this.other = Boolean.TRUE;
                    }
                } else if(this.parts.length == 10) {
                    if(this.parts[4].contains("A HIC Number:")) {
                        this.medicare = Boolean.TRUE;
                        this.other = Boolean.FALSE;
                    }
                } else if(this.parts.length == 11) {
                    if(this.parts[10].contains("Provider Name")) {
                        this.ltc = Boolean.TRUE;
                        this.other = Boolean.FALSE;
                        this.program = this.parts[1].replace(String.valueOf(' '), "").trim();
                        this.programCode = this.parts[2].replace(String.valueOf(' '), "").trim();
                        this.status = this.parts[3].replace(String.valueOf(' '), "").trim();
                        this.beginDate = this.parts[4].replace(String.valueOf(' '), "").trim();
                        this.endDate = this.parts[5].replace(String.valueOf(' '), "").trim();
                    } else {
                        this.other = Boolean.TRUE;
                    }
                } else if(this.parts.length == 13) {
                    if(this.parts[7].contains("MCO")) {
                        this.mco = Boolean.TRUE;
                        this.mcoName = this.parts[10].replace(String.valueOf(' '), "").trim();
                        program = this.parts[1].replace(String.valueOf(' '), "");
                        programCode = this.parts[2].replace(String.valueOf(' '), "");
                        status = this.parts[3].replace(String.valueOf(' '), "");
                        beginDate = this.parts[4].replace(String.valueOf(' '), "");
                        endDate = this.parts[5].replace(String.valueOf(' '), "");
                        qmbCode = this.parts[6].replace(String.valueOf(' '), "");
                        this.program = program.trim();
                        this.programCode = programCode.trim();
                        this.status = status.trim();
                        this.beginDate = beginDate.trim();
                        this.endDate = endDate.trim();
                        this.qmbCode = qmbCode.trim();
                        this.uninsured = Boolean.FALSE;
                        this.other = Boolean.FALSE;
                    } else {
                        this.other = Boolean.TRUE;
                    }
                } else if(this.parts.length == 16) {
                    this.mco = this.parts[7].contains("MCO");
                    this.medicare = this.parts[13].contains("A HIC Number:");
                    if(!this.medicare && !this.mco) {
                        this.other = Boolean.TRUE;
                    } else {
                        this.mco = Boolean.TRUE;
                        this.mcoName = this.parts[10].replace(String.valueOf(' '), "").trim();
                        program = this.parts[1].replace(String.valueOf(' '), "");
                        programCode = this.parts[2].replace(String.valueOf(' '), "");
                        status = this.parts[3].replace(String.valueOf(' '), "");
                        beginDate = this.parts[4].replace(String.valueOf(' '), "");
                        endDate = this.parts[5].replace(String.valueOf(' '), "");
                        qmbCode = this.parts[6].replace(String.valueOf(' '), "");
                        this.program = program.trim();
                        this.programCode = programCode.trim();
                        this.status = status.trim();
                        this.beginDate = beginDate.trim();
                        this.endDate = endDate.trim();
                        this.qmbCode = qmbCode.trim();
                        this.uninsured = Boolean.FALSE;
                        this.other = Boolean.FALSE;
                    }
                } else if(this.parts.length == 19) {
                    this.medicare = this.parts[13].contains("A HIC Number:");
                    this.mco = this.parts[10].contains("MCO");
                    if(!this.medicare && !this.mco) {
                        this.other = Boolean.TRUE;
                    } else {
                        this.mco = Boolean.TRUE;
                        this.mcoName = this.parts[10].replace(String.valueOf(' '), "").trim();
                        program = this.parts[1].replace(String.valueOf(' '), "");
                        programCode = this.parts[2].replace(String.valueOf(' '), "");
                        status = this.parts[3].replace(String.valueOf(' '), "");
                        beginDate = this.parts[4].replace(String.valueOf(' '), "");
                        endDate = this.parts[5].replace(String.valueOf(' '), "");
                        qmbCode = this.parts[6].replace(String.valueOf(' '), "");
                        this.program = program.trim();
                        this.programCode = programCode.trim();
                        this.status = status.trim();
                        this.beginDate = beginDate.trim();
                        this.endDate = endDate.trim();
                        this.qmbCode = qmbCode.trim();
                        this.uninsured = Boolean.FALSE;
                        this.other = Boolean.FALSE;
                    }
                } else {
                    this.other = Boolean.TRUE;
                }
            }
        } else {
            this.other = Boolean.TRUE;
        }

    }

    private String removeServices(String rawResults) {
        Integer removeStart = rawResults.indexOf("Service types");
        if(removeStart > 0) {
            Integer mcoRemoveEnd = rawResults.indexOf("Service Management");
            Integer tplRemoveEnd = rawResults.indexOf("Third Party Liability");
            Integer medicareRemoveEnd = rawResults.indexOf("Medicare Information");
            Integer ltcRemoveEnd = rawResults.indexOf("Long Term Care");
            ArrayList<Integer> ends = new ArrayList<>();
            ends.add(medicareRemoveEnd);
            ends.add(tplRemoveEnd);
            ends.add(ltcRemoveEnd);
            ends.add(mcoRemoveEnd);
            ends.add(rawResults.length());
            ends.removeAll(Collections.singleton(-1));
            Collections.sort(ends);
            Integer end = (Integer)ends.get(0);
            if(end > removeStart) {
                try {
                    String e = rawResults.substring(removeStart, end);
                    rawResults = rawResults.replace(e, "");
                } catch (IndexOutOfBoundsException var10) {
                    var10.printStackTrace();
                }
            }
        }

        return rawResults;
    }

    private String[] standardize(String rawResults) {
        rawResults = rawResults.replace("Plan Coverage Information Plan Coverage:", "x");
        rawResults = rawResults.replace("Plan Coverage Information", "x");
        rawResults = rawResults.replace("Program Code:", "x");
        rawResults = rawResults.replace("Eligibility or Benefit Information:", "x");
        rawResults = rawResults.replace("Medicare Information", "x");
        rawResults = rawResults.replace("Third Party Liability Information", "x");
        rawResults = rawResults.replace("Begin Date:", "x");
        rawResults = rawResults.replace("End Date:", "x");
        rawResults = rawResults.replace("QMB Indicator:", "x");
        rawResults = rawResults.replace("Service Management Service Management Type:", "x");
        rawResults = rawResults.replace("Provider:", "x");
        rawResults = rawResults.replace("Service types", "x");
        rawResults = rawResults.replace("Part A/B Indicator:", "x");
        rawResults = rawResults.replace("Service Management", "x");
        rawResults = rawResults.replace("Long Term Care Information", "x");
        return rawResults.split("x");
    }
}
