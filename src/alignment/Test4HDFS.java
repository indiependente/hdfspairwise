package alignment;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

import hdfs.matrix.HDFSMatrixBlock;
import hdfs.matrix.HDFSMatrixManager;

public class Test4HDFS {
	
	public static void main(String[] args) throws IOException, IncompatibleScoringSchemeException{
	
	
	HDFSSmithWaterman sw = new HDFSSmithWaterman();
	
	try {
		sw.loadSequences(new FileReader(args[0]), new FileReader(args[1]));
	} catch (InvalidSequenceException e1) {
		System.err.println("Error in loading sequences");
	}

	
	
	sw.setScoringScheme(new BasicScoringScheme(1,-1,-1));
	PairwiseAlignment pw= sw.computePairwiseAlignment();
//	System.out.println(pw.toStringSpl());
	
//	PrintWriter stampa = new PrintWriter("Output_algo_splitted");
	System.out.println(pw.toStringSpl());
//	stampa.print(pw.toStringSpl());
//	stampa.close();
	
	
//	stampa=null;
	
	
	
	/*SmithWaterman naivesmith = new SmithWaterman();
	try {
		naivesmith.loadSequences(new FileReader(args[0]), new FileReader(args[1]));
	} catch (InvalidSequenceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	naivesmith.setScoringScheme(new BasicScoringScheme(1,-1,-1));
//	naivesmith.computePairwiseAlignment();
	PairwiseAlignment nw = naivesmith.computePairwiseAlignment();
//	stampa = new PrintWriter("Output_algo_naive_splitted");
	System.out.println(nw.toString());
//	stampa.print(nw.toStringSpl());
//	stampa.close();
//	System.out.println(nw.gapped_seq1.length());
	
	
//	naivesmith.computeMatrix();
	
//	System.out.println("Matrix hdfs");
	
	
	
	
	/*
	
	//test correctness of matrix
	HDFSMatrixManager.getInstance().setup(8000, 8000);
	HDFSMatrixBlock blk = HDFSMatrixManager.getInstance().readFromHDFS(3);

		System.out.println("begin test");
		for(int i=0;i<4000;i++){
			if(naivesmith.matrix[4000+i][4000]!=blk.get(0, i))System.out.println("error");
		}
		System.out.println();
	
	System.out.println("After!");
	
	PairwiseAlignment nw = naivesmith.buildOptimalAlignment();
	
	System.out.println(nw.toString());
	System.out.println(nw.gapped_seq1.length());
//	System.out.println(naivesmith.getScore());
	
/*************************************************
	
	System.out.println("Test of matrices");
	
	HDFSSmithWaterman sw = new HDFSSmithWaterman();
	
	try {
		sw.loadSequences(new FileReader(args[0]), new FileReader(args[1]));
	} catch (InvalidSequenceException e1) {
	}	
	sw.setScoringScheme(new BasicScoringScheme(1,-1,-1));
	sw.computeMatrix();
	
	SmithWaterman naivesmith = new SmithWaterman();
	try {
		naivesmith.loadSequences(new FileReader(args[0]), new FileReader(args[1]));
	} catch (InvalidSequenceException e) {
	}
	
	naivesmith.setScoringScheme(new BasicScoringScheme(1,-1,-1));
	naivesmith.computeMatrix();
	
	
	HDFSMatrixBlock blk = HDFSMatrixManager.getInstance().readFromHDFS(0);
	
	
	blk=null;
	System.gc();
	
	blk= HDFSMatrixManager.getInstance().readFromHDFS(1);
	
	for(int i=0;i<4000;i++){
			if(naivesmith.getMatrix()[i][4000]!=blk.get(i, 0))System.out.println("err");
	}
	
	blk=null;
	System.gc();
	
	blk= HDFSMatrixManager.getInstance().readFromHDFS(2);
	
	for(int i=4000;i<8000;i++){
		for(int j=0;j<4000;j++){
			if(naivesmith.getMatrix()[i][j]!=blk.get(i, j%4000));
		}
	}
	
	
	System.out.println("block 2 ok");
	
	
	blk=null;
	System.gc();
	
	blk= HDFSMatrixManager.getInstance().readFromHDFS(3);
	for(int i=4000;i<8000;i++){
		
		for(int j=4000;j<8000;j++){
			if(naivesmith.getMatrix()[i][j]!=blk.get(i%4000, j%4000));
		}
	}
	System.out.println("block 3 ok");
	
//	System.out.println("score 2 = "+naivesmith.getScore()+" coords["+naivesmith.max_row+","+naivesmith.max_col+"].");
	*****************/
	/*
	for(int i=0;i<=2;i++){
		HDFSMatrixBlock b = HDFSMatrixManager.getInstance().readFromHDFS(i);
		System.out.println("Matrice "+i);
		for(int j=0;j<b.getWidth();j++){
			System.out.print(b.get(0, j)+" - ");
		}
		System.out.println("width = "+b.getWidth());
	}*/
	
	/*SmithWaterman sw = new SmithWaterman();
	try {
		sw.loadSequences(new FileReader(args[0]),new FileReader(args[1]));
	} catch (InvalidSequenceException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/
	
	
	
	
	
	/*
	sw.setScoringScheme(new BasicScoringScheme(1, -1, -1));
	sw.computeMatrix();
	
	System.out.println(sw.getScore());
	
	
	HDFSMatrixBlock blk = new HDFSMatrixBlock(0,0,0, 10, 10, 100);
	
	for(int i=0;i<10;i++){
		for(int j=0;j<10;j++){
			blk.set(r.nextInt(), i, j);
		}
	}*/
	
	
	/*
	//HDFSMatrixManager.getInstance().setup(8192, 12288);
	//HDFSMatrixManager.getInstance().setup(8192, 8192);
	HDFSMatrixManager.getInstance().setup(8192, 10000);
	//HDFSMatrixManager.getInstance().setup(8000, 12000);
	
	int bc = HDFSMatrixManager.getInstance().getBlockCount();
	int bec = HDFSMatrixManager.getInstance().getBlockElementsCount();
	//int tot = bc*bec;
	int tot = 8192*10000;
	int glob_count = 0;
	System.out.println("tot: "+tot);
	
	
	
	int w,h;
	
	while(glob_count<tot){
	
	HDFSMatrixBlock blk = HDFSMatrixManager.getInstance().getNextBlock();
	w=blk.getWidth();
	h=blk.getHeight();
	System.out.println("block width: "+w+" block height: "+h);
	System.out.println("id = "+blk.getId()+" xRealoffset= "+blk.getxOffset()+" yRealoffset= "+blk.getyOffset());
	System.out.println("Nblock ="+HDFSMatrixManager.getInstance().getNblock());
	System.out.println("Mblock ="+HDFSMatrixManager.getInstance().getMblock());
	
	for(int i=0;i<w;i++){
		for(int j=0;j<h;j++){
			try {
				blk.set(r.nextInt(10), i, j);
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				System.out.println("i " + i + " " + j);
				throw e;
			}
		}
	}
	
	glob_count+=w*h;
	System.out.println("NEW GLOBAL COUNT : "+glob_count);
	HDFSMatrixManager.getInstance().writeOnHDFS(blk);
	}
	
	
	
//	HDFSMatrixManager.getInstance().closeOutStream();
	
	System.out.println("(2,1) = 5? "+HDFSMatrixManager.getInstance().getIdByOffsets(2, 1));
	System.out.println("calcolo le dipendenze per il blocco 5");
	ArrayList<Integer>ids=HDFSMatrixManager.getInstance().getDependenciesForBlock(5);
	for(int i=0;i<ids.size();i++){
		System.out.println("elemento "+i+"="+ids.get(i));
	}
	System.out.println("calcolo le dipendenze per il blocco 3");

	ArrayList<Integer>ids1=HDFSMatrixManager.getInstance().getDependenciesForBlock(3);
	for(int i=0;i<ids1.size();i++){
		System.out.println("elemento "+i+"="+ids1.get(i));
	}
	
	HDFSMatrixBlock ret;
	ret = HDFSMatrixManager.getInstance().readFromHDFS(1);
	
	PrintWriter pw = new PrintWriter("output.txt");
	
	int[][]mat = ret.getMatrix();
	pw.println("\n----------ID 1-------\n\n\n\n\n\n");
	
	
	
	/*for(int k=0;k<ret.getWidth();k++){
		pw.print(k+" ");
	}
	System.out.println();
	
	for(int k=0;k<ret.getWidth();k++){
		pw.print(k+"|");
		for(int n=0;n<ret.getHeight();n++){
			pw.print(mat[k][n]+" ");
		}
		pw.println("\n");
	}
	
	pw.println("\n----------ID 3\n\n\n\n\n");
	
	/*for(int k=0;k<ret.getWidth();k++){
		pw.print(k+" ");
	}
	pw.println();
	ret = HDFSMatrixManager.getInstance().readFromHDFS(3);
	
	for(int k=0;k<ret.getWidth();k++){
		pw.print(k+"|");
		for(int n=0;n<ret.getHeight();n++){
			pw.print(mat[k][n]+" | ");
		}
		pw.println("\n");
	}*/
	
	
	
	
	}
	
	
	
	
	
}
