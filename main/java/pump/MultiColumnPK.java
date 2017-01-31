package pump;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.LinkedHashMap;
import java.util.List;

public class MultiColumnPK implements PrimaryKey {

    private List<String> pkColumnName;
    private LinkedHashMap<String,Integer> pkValues;

    public MultiColumnPK(List<String> pkColumnName, LinkedHashMap<String,Integer> pkValues){
        this.pkColumnName = pkColumnName;
        this.pkValues = pkValues;
    }

    @Override
    public Integer getFirstKeyValue(){
        return pkValues.get(pkColumnName.get(0));
    }

    @Override
    public Object getKeyName() {
        return pkColumnName;
    }

    @Override
    public Object getKeyValue() {
        return pkValues;
    }

    @Override
    public boolean equals(Object object){
        if(!(object instanceof MultiColumnPK)){
            return false;
        }
        if(object == this) {
            return true;
        }
        EqualsBuilder builder = new EqualsBuilder();
        MultiColumnPK rhs = (MultiColumnPK) object;
        LinkedHashMap<String,Integer> rhsKey = rhs.pkValues;
        for(String columnName : pkColumnName){
            builder.append(pkValues.get(columnName),rhsKey.get(columnName));
        }
        return builder.isEquals();
    }

    @Override
    public int hashCode(){
        HashCodeBuilder builder = new HashCodeBuilder(17,31);
        for(String key : pkValues.keySet()){
            builder.append(pkValues.get(key));
        }
        return builder.toHashCode();
    }
}
