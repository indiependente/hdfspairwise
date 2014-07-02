package hdfs.matrix;

import hdfs.ConfigurationLoader;

import java.util.ArrayList;

public class HDFSMatrixManager {
	private HDFSMatrixBlock currentBlock;
	private int currentBlockId;
	private static final int START_BLOCK_INDEX = -1;
	private static HDFSMatrixManager instance = null;
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

	
	private HDFSMatrixManager(){
		currentBlockId = START_BLOCK_INDEX;
		currentBlock = null;
		blockCount = 0;
		blockSize = ConfigurationLoader.getInstance().getIntValue(ConfigurationLoader.BLOCK_SIZE); //64 * (1 << 20);
		blockElementsCount = 0;
	}
	

	public int getBlockLength() {
		return blockElementsCount;
	}

	public static HDFSMatrixManager getInstance(){
		if(instance == null)
			instance = new HDFSMatrixManager();
		
		return instance;
	}
	
	public void setup(int length1, int length2){
//		int elementsPerBlock = blockSize >> INTEGER_SIZE_LOG; // elements in a block
		this.blockElementsCount = blockSize >> INTEGER_SIZE_LOG;
		int elementsInBlockLine = (int) Math.sqrt(blockElementsCount);
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
		int elementsInBlockLine = (int) Math.sqrt(blockElementsCount);
//		int x = (int) (currentBlockId % blocksPerLine);
		
		int xOffset = currentBlockId % nBlock;
		int xRealOffset = xOffset * elementsInBlockLine;
//		int y = (currentBlockId - x) / blocksPerLine;
		
		int yOffset = (currentBlockId - xOffset) / mBlock;
		int yRealOffset = yOffset * elementsInBlockLine;
		/**
		 * Block width. Keeping in mind of matrix edges.
		 */
		int w = (xOffset == (nBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * nBlock - length1) : elementsInBlockLine;
		/**
		 * Block height. Keeping in mind of matrix edges.
		 */
		int h = (xOffset == (mBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * mBlock - length2) : elementsInBlockLine;
		
		return (currentBlock = new HDFSMatrixBlock(currentBlockId, xRealOffset, yRealOffset, w, h, blockElementsCount)); 
	}

	public int getLength1() {
		return length1;
	}

	public int getLength2() {
		return length2;
	}


	public void storeMatrixBlockSize(int id, int width, int height) {
		// TODO Auto-generated method stub
		
	}


	public int[] getMatrixBlockSize(int id) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
}
