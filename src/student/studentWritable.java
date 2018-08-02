package student;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class studentWritable implements WritableComparable<studentWritable> {

	private Text name;
	private Text gender;
	
	public studentWritable() {
		set(new Text(),new Text());
	}
	
	public studentWritable(Text name,Text gender) {
		this.name=name;
		this.gender=gender;
	}
	
	public void set(Text name,Text gender) {
		this.name=name;
		this.gender=gender;
	}
	public Text getName() {
		return name;
	}

	public Text getGender() {
		return gender;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		name.readFields(input);
		gender.readFields(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		name.write(output);
		gender.write(output);
		
	}

	@Override
	public int compareTo(studentWritable o) {
		int status = this.name.compareTo(o.name);
		if(status!=0) {
			return status;
		}
		return this.gender.compareTo(o.gender);
		
	}
	@Override
	public String toString() {
		return name+","+gender;
	}

}
