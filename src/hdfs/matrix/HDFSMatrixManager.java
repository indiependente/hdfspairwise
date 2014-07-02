package hdfs.matrix;

import hdfs.ConfigurationLoader;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class HDFSMatrixManager {
	private HDFSMatrixBlock currentBlock;
	private int currentBlockId;
	private static final int START_BLOCK_INDEX = -1;
	private static HDFSMatrixManager instance = null;
	private HashMap<Integer, int[]> sizeMap;
	
	/**
	 * Total block number
	 */
	private int blockCount;
	/**
	 * Block size in bytes
	 */
	private int blockSize;
	private int length1;
	private int length2;
	private int elementsInBlockLine;
	
	private FileSystem fs;
	private FSDataInputStream in;
	private FSDataOutputStream out;
	private Path toHdfs;
	
	/**
	 * Number of elements in a block
	 */
	private int blockElementsCount;
	/**
	 * Number of blocks in a row of the global matrix
	 */
//	private int blocksPerLine;
	/**
	 * Number of blocks in a row
	 */
	private int nBlock;
	/**
	 * Number of blocks in a column
	 */
	private int mBlock;
	
	private static final int INTEGER_SIZE = 4;
	private static final int INTEGER_SIZE_LOG = 2;

	
	private HDFSMatrixManager() throws IOException{
		currentBlockId = START_BLOCK_INDEX;
		currentBlock = null;
		blockCount = 0;
		blockSize = ConfigurationLoader.getInstance().getIntValue(ConfigurationLoader.BLOCK_SIZE); //64 * (1 << 20);
		blockElementsCount = 0;
		sizeMap = new HashMap<Integer, int[]>();
		
		/*
		 * 
		 */
		toHdfs = new Path(ConfigurationLoader.HADOOP_HOME+"matrice");
		Configuration conf = new Configuration();
		fs = FileSystem.get(conf);
		
		
	}
	

	public int getBlockLength() {
		return blockElementsCount;
	}

	public static HDFSMatrixManager getInstance() throws IOException{
		if(instance == null)
			instance = new HDFSMatrixManager();
		
		return instance;
	}
	
	public void setup(int length1, int length2){
//		int elementsPerBlock = blockSize >> INTEGER_SIZE_LOG; // elements in a block
		this.blockElementsCount = blockSize >> INTEGER_SIZE_LOG;
		elementsInBlockLine = (int) Math.sqrt(blockElementsCount);
		this.nBlock = ((int) (length1 / elementsInBlockLine)) + clamp(length1 % elementsInBlockLine, 0, 1); //rows
		this.mBlock = ((int) (length2 / elementsInBlockLine)) + clamp(length2 % elementsInBlockLine, 0, 1); //columns
		this.blockCount = nBlock * mBlock;
//		this.blocksPerLine = (int) Math.sqrt(blockCount);
		this.length1 = length1;
		this.length2 = length2;
		
	}
	
	private int clamp(int x, int low, int high){
		return (x == low) ? low : high;
	}
	
	public HDFSMatrixBlock getNextBlock(){
		currentBlockId++;
//		int x = (int) (currentBlockId % blocksPerLine);
		currentBlock = createBlock(currentBlockId);
		return currentBlock; 
	}
	
	
	public HDFSMatrixBlock createBlock(int id){
		int xOffset = id % nBlock;
		int xRealOffset = xOffset * elementsInBlockLine;
//		int y = (currentBlockId - x) / blocksPerLine;
		
		int yOffset = (id - xOffset) / mBlock;
		int yRealOffset = yOffset * elementsInBlockLine;
		/**
		 * Block width. Keeping in mind of matrix edges.
		 */
		int w = (xOffset == (nBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * nBlock - length1) : elementsInBlockLine;
		/**
		 * Block height. Keeping in mind of matrix edges.
		 */
		int h = (xOffset == (mBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * mBlock - length2) : elementsInBlockLine;
		
		return (new HDFSMatrixBlock(id, xRealOffset, yRealOffset, w, h, blockElementsCount));
	}

	public int getLength1() {
		return length1;
	}

	public int getLength2() {
		return length2;
	}

	public void storeMatrixBlockSize(int id, int width, int height){
		
		if(width!=elementsInBlockLine || height!=elementsInBlockLine){		
			int[]array=new int[2];
			array[0]=width;
			array[1]=height;
			sizeMap.put(id, array);
		}
		
	}


	public int[] getMatrixBlockSize(int id) {
		
		int [] toReturn = sizeMap.get(id);
		if(toReturn!=null)return toReturn;
		toReturn = new int[2];
		toReturn[0] = elementsInBlockLine;
		toReturn[1] = elementsInBlockLine;
		return toReturn;
		
	}
	
	//dopo aver computato il blocco scrivererlo sull'hdfs ???
	public void writeOnHDFS(HDFSMatrixBlock block) throws IOException{
		//out = fs.create(toHdfs);
		out = fs.append(toHdfs);
		block.write(out);
		out.close();
	}
	
	public HDFSMatrixBlock readFromHDFS(int id) throws IOException{
		in = fs.open(toHdfs);
		in.seek(id*blockSize);
		HDFSMatrixBlock m = createBlock(id);
		m.readFields(in);
		return m;
	}
	
	
	//Occorre effettuare il calcolo delle dipendenze
	
	
	
	
}
