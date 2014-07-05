package hdfs.matrix;

import hdfs.ConfigurationLoader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class HDFSMatrixManager {
//	private static final int meta = 16388;
	private HDFSMatrixBlock currentBlock;
	private int currentBlockId;
	private static final int START_BLOCK_INDEX = -1;
	private static HDFSMatrixManager instance = null;
	private HashMap<Integer, int[]> sizeMap;
	private int[] last_left_line;
	
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
//		blockSize = 67108864;
		blockElementsCount = 0;
		sizeMap = new HashMap<Integer, int[]>();
		last_left_line=new int[4001];
		
//		toHdfs = new Path("matrice");
//		System.out.println(toHdfs);
		
		fs = FileSystem.get(ConfigurationLoader.getInstance().getConfiguration());
//		out = fs.create(toHdfs);		uno per ogni file
		
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
		this.blockElementsCount = 4000 * 4000; //blockSize >> INTEGER_SIZE_LOG;		//Number of elements in a block
		elementsInBlockLine = (int) Math.sqrt(blockElementsCount);
		this.nBlock = ((int) (length1 / elementsInBlockLine)) + clamp(length1 % elementsInBlockLine, 0, 1); //Macro-blocks in a row
		this.mBlock = ((int) (length2 / elementsInBlockLine)) + clamp(length2 % elementsInBlockLine, 0, 1); //Macro-blocks in a column
		this.blockCount = nBlock * mBlock;	//Number of Macro-blocks
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
		int xOffset = id % nBlock;								//Offset relativo al blocco
		int xRealOffset = xOffset * elementsInBlockLine;		//Offset relativo all'intera matrice
	//		int y = (currentBlockId - x) / blocksPerLine;
		
		//int yOffset = ((id - xOffset) / mBlock);			//nessun problema con matrice quadrata
		int yOffset = id/nBlock;
		
		int yRealOffset = yOffset * elementsInBlockLine;
		/**
		 * Block width. Keeping in mind of matrix edges.
		 */
		int w = (yOffset == (mBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * mBlock - length2) : elementsInBlockLine;
		/**
		 * Block height. Keeping in mind of matrix edges.
		 */
		int h = (xOffset == (nBlock - 1)) ? elementsInBlockLine - (elementsInBlockLine * nBlock - length1) : elementsInBlockLine;
		
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
		//if(fs.exists(toHdfs))out = fs.append(toHdfs);
		//else 
		out=fs.create(new Path(block.getId()+""));
//		System.out.println(out.getPos());
		block.write(out);
		out.close();
	}
	
	public HDFSMatrixBlock readFromHDFS(int id) throws IOException{
//		in = fs.open(toHdfs);
		in = fs.open(new Path(id+""));
		//in.seek(id*blockSize);
//		in.seek(id*(blockSize+meta));
		HDFSMatrixBlock m = createBlock(id);
		m.readFields(in);
		return m;
	}
	
	
	/**
	 	*
	 	* Returns the blockids list of the dependencies for the block id.
	 	* @param id The block to look for dependencies
	 	* @return The list of dependencies
	 	*
	 	*
	 	* 		 x
	 	* 	 ------->
	 	*   |
	 	* y |
	 	* 	|
	 	* 	v
	 	*
	 	*
	 	*/
	
	public ArrayList<Integer> getDependenciesForBlock(int id){
		int xOffset = id % nBlock;
//		int yOffset = (id - xOffset) / mBlock;
		int yOffset = id/nBlock;
		ArrayList<Integer> depList = new ArrayList<Integer>();

		if (xOffset == 0 && yOffset == 0)
			return depList;

		if (yOffset == 0 && xOffset != 0) // go left
			depList.add(getIdByOffsets(xOffset - 1, yOffset));

		else if(xOffset == 0 && yOffset != 0) // go up
			depList.add(getIdByOffsets(xOffset, yOffset - 1));

		else if(xOffset != 0 && yOffset != 0){
			depList.add(getIdByOffsets(xOffset - 1, yOffset)); // go left
			depList.add(getIdByOffsets(xOffset, yOffset -1)); // go up
			depList.add(getIdByOffsets(xOffset -1 , yOffset -1)); // go diag
	}

		return depList;
}
	
	public int getIdByOffsets(int xOff, int yOff){
//		return (xOff + yOff * ((int) Math.sqrt(blockElementsCount)));
		return (nBlock*yOff)+xOff;
	}
	
	
	public int getBlockCount(){
		return blockCount;
	}
	
	public int getBlockElementsCount(){
		return blockElementsCount;
	}
	
	public void closeOutStream() throws IOException{
		out.close();
	}


	public int getNblock() {
		return nBlock;
	}
	
	public int getMblock(){
		return mBlock;
	}
	
	public void setLLLine(int val,int pos){
		last_left_line[pos]=val;
	}
	
	public int[] getLLLine(){
		return last_left_line;
	}
	
}
