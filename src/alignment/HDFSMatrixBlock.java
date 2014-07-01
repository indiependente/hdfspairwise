package alignment;

public class HDFSMatrixBlock {
	private int id;
	private int width;
	private int height;
	private int xOffset;
	private int yOffset;
	private int blockElementsCount;
	private int[][] matrix;
	
	public HDFSMatrixBlock(int id, int x, int y, int width, int height, int blockElementsCount){
		this.id = id;
		this.width = width;
		this.height = height;
		this.xOffset = x;
		this.yOffset = y;
		this.blockElementsCount = blockElementsCount;
		matrix = new int[this.width][this.height];
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
}
