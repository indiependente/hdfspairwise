package alignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

public class HDFSMatrixBlock implements Writable {
	private int id;
	private int width;
	private int height;
	private int xOffset;
	private int yOffset;
	private int blockElementsCount;
	private IntWritable[][] matrix;
	
	public HDFSMatrixBlock(int id, int x, int y, int width, int height, int blockElementsCount){
		this.id = id;
		this.width = width;
		this.height = height;
		this.xOffset = x;
		this.yOffset = y;
		this.blockElementsCount = blockElementsCount;
		this.matrix = new IntWritable[this.width][this.height];
	}
	
	public void finalize(){
		matrix = null;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public int getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public int getxOffset() {
		return xOffset;
	}
	public void setxOffset(int xOffset) {
		this.xOffset = xOffset;
	}
	public int getyOffset() {
		return yOffset;
	}
	public void setyOffset(int yOffset) {
		this.yOffset = yOffset;
	}

	public int getBlockElementsCount() {
		return blockElementsCount;
	}
	
	public void readFields(DataInput stream) throws IOException {
		Writable[][] tempMatrix = null;
		TwoDArrayWritable tmp = new TwoDArrayWritable(IntWritable.class);
		tmp.readFields(stream);
		tempMatrix = tmp.get();
		for (int i = 0; i < tempMatrix.length; i++)
		{
			for (int j = 0; j < tempMatrix.length; j++)
			{
				matrix[i][j].set(((IntWritable) tempMatrix[i][j]).get());
			}
		}
		System.gc();
	}

	public void write(DataOutput stream) throws IOException {
		(new TwoDArrayWritable(IntWritable.class, matrix)).write(stream);
		System.gc();
	}
	
	
}
