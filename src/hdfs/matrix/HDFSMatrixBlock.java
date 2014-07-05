package hdfs.matrix;

import hdfs.ConfigurationLoader;

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
	private int blockSize;
	private int[][] matrix;
	
	public HDFSMatrixBlock(int id, int x, int y, int width, int height, int blockElementsCount){
		this.id = id;
		this.width = width;
		this.height = height;
		this.xOffset = x;
		this.yOffset = y;
		this.blockElementsCount = blockElementsCount;
		this.blockSize = ConfigurationLoader.getInstance().getIntValue(ConfigurationLoader.BLOCK_SIZE);
//		this.blockSize = 67108864;
		this.matrix = new int[this.width][this.height]; //new int[width][height];
	}
	
	public void finalize() {
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
		int[] size = HDFSMatrixManager.getInstance().getMatrixBlockSize(id);
		width = size[0];
		height = size[1];
		tmp.readFields(stream);
		tempMatrix = tmp.get();
		for (int i = 0; i < width; i++)
		{
			for (int j = 0; j < height; j++)
			{
				matrix[i][j] = ((IntWritable) tempMatrix[i][j]).get();
			}
		}
		System.gc();
	}

	
	//write sposta direttamente il cursore quindi non mi serve la read
	public void write(DataOutput stream) throws IOException {
		HDFSMatrixManager.getInstance().storeMatrixBlockSize(id, width, height);
		IntWritable[][] tempMatrix = new IntWritable[width][height];
		for (int i = 0; i < width; i++)
		{
			for (int j = 0; j < height; j++)
			{
				tempMatrix[i][j] = new IntWritable(matrix[i][j]);
			}
		}
		(new TwoDArrayWritable(IntWritable.class, tempMatrix)).write(stream);
		
		System.gc();
	}
	
	public int get(int i,int j){
		return matrix[i][j];
	}
	
	public void set(int x,int i,int j){
		
		matrix[i][j]=x;
	}
	
	public int[][] getMatrix(){
		return matrix;
	}
	
	
}
