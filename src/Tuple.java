import java.io.Serializable;
import java.util.ArrayList;

public class Tuple implements Serializable {
	
	ArrayList<Object> values;
	
	public Tuple(ArrayList<Object> values) {
		this.values= new ArrayList<Object>();
		for(Object value:values) {
			this.values.add(value);
		}
	}
	
	public String toString() {
		String Values="";
		for(Object value:this.values) {
			Values+=value.toString()+",";
		}
		return Values;
	}
	

}