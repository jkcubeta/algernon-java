package pump;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SingleColumnPK implements PrimaryKey{

    private String pkColumnName;
    private Integer pkValue;

    public SingleColumnPK(String pkColumnName, Integer pkValue){
        this.pkColumnName = pkColumnName;
        this.pkValue = pkValue;
    }

    @Override
    public Integer getFirstKeyValue(){
        return pkValue;
    }

    @Override
    public Object getKeyName() {
        return pkColumnName;
    }

    @Override
    public Object getKeyValue() {
        return pkValue;
    }

    @Override
    public boolean equals(Object object){
        if(!(object instanceof SingleColumnPK)){
            return false;
        }

        if(object == this) {
            return true;
        }

        EqualsBuilder builder = new EqualsBuilder();
        SingleColumnPK rhs = (SingleColumnPK) object;
        builder.append(pkValue,rhs.pkValue);
        return builder.isEquals();
    }

    @Override
    public int hashCode(){
        HashCodeBuilder builder = new HashCodeBuilder(17,31);
        builder.append(pkValue);
        return builder.toHashCode();
    }
}
