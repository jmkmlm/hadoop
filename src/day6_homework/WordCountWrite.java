package day6_homework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordCountWrite implements WritableComparable<WordCountWrite> {
	private Text name;
	private Text age;
	
	public WordCountWrite() {
		set(new Text(),new Text());
	}
	public void set(Text name,Text age) {
		this.name=name;
		this.age=age;
	}
	public WordCountWrite(Text name,Text age) {
		this.name=name;
		this.age=age;
	}
	public Text getName() {
		return name;
	}

	public Text getAge() {
		return age;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		name.readFields(input);
		age.readFields(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		name.write(output);
		age.write(output);
		
	}

	@Override
	public int compareTo(WordCountWrite o) {
		int status = name.compareTo(o.name);//相同返回0
		if(status!=0) {
			return status;
		}
		return age.compareTo(o.age);
	}

	@Override
	public String toString() {
		return this.name+","+this.age;
	}
}
