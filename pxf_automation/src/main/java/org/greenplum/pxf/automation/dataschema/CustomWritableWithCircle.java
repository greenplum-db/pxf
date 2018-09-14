package org.greenplum.pxf.automation.dataschema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomWritableWithCircle
  implements Writable
{
  public int int1;
  public String circle;

  public CustomWritableWithCircle()
  {
    this.int1 = 0;

    this.circle = new String();
  }

  public CustomWritableWithCircle(int paramInt1, int paramInt2, int paramInt3)
  {
    this.int1 = paramInt1;
    this.circle = ("< ( " + paramInt2 + " , " + paramInt3 + " ) , " + paramInt1 * paramInt2 * paramInt3 + " >");
  }

  int GetInt1()
  {
    return this.int1;
  }

  String GetCircle()
  {
    return this.circle;
  }

  @Override
public void write(DataOutput paramDataOutput)
    throws IOException
  {
    IntWritable localIntWritable = new IntWritable();

    localIntWritable.set(this.int1);
    localIntWritable.write(paramDataOutput);

    Text localText = new Text();
    localText.set(this.circle);
    localText.write(paramDataOutput);
  }

  @Override
public void readFields(DataInput paramDataInput)
    throws IOException
  {
    IntWritable localIntWritable = new IntWritable();

    localIntWritable.readFields(paramDataInput);
    this.int1 = localIntWritable.get();

    Text localText = new Text();
    localText.readFields(paramDataInput);
    this.circle = localText.toString();
  }

  public void printFieldTypes()
  {
    Class localClass = getClass();
    Field[] arrayOfField = localClass.getDeclaredFields();

    for (int i = 0; i < arrayOfField.length; i++)
    {
      System.out.println(arrayOfField[i].getType().getName());
    }
  }
}
