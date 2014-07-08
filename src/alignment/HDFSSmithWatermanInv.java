package alignment;


import hdfs.matrix.HDFSMatrixBlock;
import hdfs.matrix.HDFSMatrixManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

public class HDFSSmithWatermanInv extends PairwiseAlignmentAlgorithm
{



	/**
	 * The first sequence of an alignment.
	 */
	protected CharSequence seq1;

	/**
	 * The second sequence of an alignment.
	 */
	protected CharSequence seq2;

	/**
	 * The dynamic programming matrix. Each position (i, j) represents the best score
	 * between a suffic of the firsts i characters of <CODE>seq1</CODE> and a suffix of
	 * the first j characters of <CODE>seq2</CODE>.
	 */
	protected int[][] matrix;

	/**
	 * Indicate the row of where an optimal local alignment can be found in the matrix..
	 */
	protected int max_row;

	/**
	 * Indicate the column of where an optimal local alignment can be found in the matrix.
	 */
	protected int max_col;

	/**
	 * Loads sequences into {@linkplain CharSequence} instances. In case of any error, an
	 * exception is raised by the constructor of <CODE>CharSequence</CODE> (please check
	 * the specification of that class for specific requirements).
	 *
	 * @param input1 Input for first sequence
	 * @param input2 Input for second sequence
	 * @throws IOException If an I/O error occurs when reading the sequences
	 * @throws InvalidSequenceException If the sequences are not valid
	 * @see CharSequence
	 */
	protected void loadSequencesInternal (Reader input1, Reader input2)
			throws IOException, InvalidSequenceException
			{
		// load sequences into instances of CharSequence
		this.seq1 = new CharSequence(input1);
		this.seq2 = new CharSequence(input2);
			}

	/**
	 * Frees pointers to loaded sequences and the dynamic programming matrix so that their
	 * data can be garbage collected.
	 */
	protected void unloadSequencesInternal ()
	{
		this.seq1 = null;
		this.seq2 = null;
		this.matrix = null;
	}

	/**
	 * Builds an optimal local alignment between the loaded sequences after computing the
	 * dynamic programming matrix. It calls the <CODE>buildOptimalAlignment</CODE> method
	 * after the <CODE>computeMatrix</CODE> method computes the dynamic programming
	 * matrix.
	 *
	 * @return an optimal pairwise alignment between the loaded sequences
	 * @throws IncompatibleScoringSchemeException If the scoring scheme is not compatible
	 * with the loaded sequences.
	 * @see #computeMatrix
	 * @see #buildOptimalAlignment
	 */
	protected PairwiseAlignment computePairwiseAlignment ()
			throws IncompatibleScoringSchemeException
			{
		// compute the matrix
		try {
			computeMatrix ();
		} catch (IOException e) {
			System.err.println("Error in computation of score matrix");
			e.printStackTrace();
		}

		System.out.println("Compute matrix ok");
		
		// build and return an optimal local alignment
		PairwiseAlignment alignment=null;
		try {
			alignment = buildOptimalAlignment ();
		} catch (IOException e) {
			System.err.println("Error in build Optimal Alignment");
			e.printStackTrace();
		}

		// allow the matrix to be garbage collected
		matrix = null;

		return alignment;
			}

	/**
	 * Computes the dynamic programming matrix.
	 *
	 * @throws IncompatibleScoringSchemeException If the scoring scheme is not compatible
	 * with the loaded sequences.
	 * @throws IOException 
	 */
	protected void computeMatrix () throws IncompatibleScoringSchemeException, IOException
	{
		HDFSMatrixManager manager = HDFSMatrixManager.getInstance();
		HDFSMatrixBlock block;
		int	r, c, rows, cols, ins, sub, del, max_score;

		int k,z,w;
		//		rows = seq1.length()+1;				
		//		cols = seq2.length()+1;

		manager.setup(seq1.length()+1,seq2.length()+1);

		// keep track of the maximum score
		this.max_row = this.max_col = max_score = 0;

		//matrix = new int [rows][cols];
		block=manager.getNextBlock();

		System.out.println("Height "+block.getHeight());
		System.out.println("Width "+block.getWidth());
		System.out.println("Lunghezza file 1 -->"+seq1.length());
		System.out.println("Lunghezza file 2 -->"+seq2.length());

		/*
		 * compute block number 0
		 */

		//compute first line...

		for(k=0;k<block.getHeight();k++){
			block.set(0,0, k);
		}

		manager.setLLLine(block.get(0, block.getHeight()-1), 0);

//		System.out.println(manager.getLLLine()[5]);
		System.out.println("before compute other lines");
		//compute other lines
		for(z=1;z<block.getWidth();z++){

			block.set(0, z, 0);

			for(w=1;w<block.getHeight();w++){

				ins = block.get(z,w-1) + scoreInsertion(seq1.charAt(w));			//seq1 o seq2?
				sub = block.get(z-1,w-1) + scoreSubstitution(seq1.charAt(w),seq2.charAt(z));
				del = block.get(z-1,w) + scoreDeletion(seq2.charAt(z));


				// choose the greatest
				block.set(max (ins, sub, del, 0), z, w);

				if (block.get(z, w) > max_score)
				{
					// keep track of the maximum score
					max_score = block.get(z, w);
					this.max_row = z; this.max_col = w;	//DA TESTARE!
				}

			}
			manager.setLLLine(block.get(z, block.getHeight()-1), z);
		}

		/*************
		STAMPA DELLA MATRICE NEL BLOCCO ZERO***************
		
		System.out.println("Matrix hdfs");
		
		PrintWriter stampa = new PrintWriter("mat-hdfs");
		
		for(int l=0;l<block.getWidth();l++){
			for(int s=0;s<block.getHeight();s++){
				stampa.print(block.get(l, s)+" ");
			}
			stampa.println();
		}
		
		stampa.close();
		
		
		***************FINE STAMPA********************/
		
		manager.writeOnHDFS(block);
		block=null;
		System.gc();



		System.out.println("After first block");



		System.out.println("Nblock = "+manager.getNblock());

		//compute other blocks on first line		
		for(c=1;c<manager.getNblock();c++){

			block=manager.getNextBlock();
			System.out.println("Width = "+block.getWidth()+" Height= "+block.getHeight()+" blockId = "+block.getId());

			//compute first line...

			for(k=0;k<block.getHeight();k++){
				block.set(0,0, k);
			}

			System.out.println("before computing first column size of seq1 = "+seq1.length());	

			//compute first column
			for(z=1;z<block.getWidth();z++){

				ins = manager.getLLLine()[z] + scoreInsertion(seq1.charAt(block.getxOffset()));
				sub = manager.getLLLine()[z-1] + scoreSubstitution(seq1.charAt(block.getxOffset()),seq2.charAt(block.getyOffset()+z));
				del = block.get(z-1,0) + scoreDeletion(seq2.charAt(block.getyOffset()+z));


				// choose the greatest
				block.set(max (ins, sub, del, 0), z, 0);

				if (block.get(z, 0) > max_score)
				{
					// keep track of the maximum score
					max_score = block.get(z, 0);
					this.max_row = block.getyOffset()+z; this.max_col = block.getxOffset()+0;	//DA TESTARE!
				}
			}
			
			manager.setLLLine(block.get(0, block.getHeight()-1), 0);

			//compute the rest of matrix

			for(z=1;z<block.getWidth();z++){


				for(w=1;w<block.getHeight();w++){
					ins = block.get(z,w-1) + scoreInsertion(seq1.charAt(block.getxOffset()+w));			//seq1 o seq2?
					sub = block.get(z-1,w-1) + scoreSubstitution(seq1.charAt(block.getxOffset()+w),seq2.charAt(block.getyOffset()+z));
					del = block.get(z-1,w) + scoreDeletion(seq2.charAt(block.getyOffset()+z));


					// choose the greatest
					block.set(max (ins, sub, del, 0), z, w);

					if (block.get(z, w) > max_score)
					{
						// keep track of the maximum score
						max_score = block.get(z, w);
						this.max_row = block.getyOffset()+z; this.max_col = block.getxOffset()+w;	//DA TESTARE!
					}


				}

				manager.setLLLine(block.get(z, block.getHeight()-1), z);

			}
			
			
			manager.writeOnHDFS(block);
			block=null;
			System.gc();
		}


		int[] last_line;

		//compute from second line of macro-blocks
		System.out.println("Mblock = "+manager.getMblock());
		int diag_el;
		HDFSMatrixBlock temp;

		for(int mb=1;mb<manager.getMblock();mb++){

			block = manager.getNextBlock();

			//fill first column
			for(k=0;k<block.getWidth();k++){
				block.set(0, k, 0);
			}
			
			//read the upper block
			temp = manager.readFromHDFS(block.getId()-manager.getNblock());
			
			//read element in the corner
			diag_el = temp.get(temp.getWidth()-1, temp.getHeight()-1);
			
			//compute first line with dependencies
			
				//modificare temp in last_line.......!
				for(w=1;w<block.getHeight();w++){
					ins = block.get(0,w-1) + scoreInsertion(seq1.charAt(block.getxOffset()+w));			//seq1 o seq2?
					sub = temp.get(temp.getWidth()-1,w-1) + scoreSubstitution(seq1.charAt(block.getxOffset()+w),seq2.charAt(block.getyOffset()+0));
					del = temp.get(temp.getWidth()-1,w) + scoreDeletion(seq2.charAt(block.getyOffset()+0));				
					
				
				
				// choose the greatest
				block.set(max (ins, sub, del, 0), 0, w);

				if (block.get(0, w) > max_score)
				{
					// keep track of the maximum score
					max_score = block.get(0, w);
					this.max_row = block.getyOffset()+0; this.max_col = block.getxOffset()+w;	//DA TESTARE!
				}
				
				}
				
				manager.setLLLine(block.get(0, block.getHeight()-1), 0);
				
				//compute the rest of matrix
				
				for(z=1;z<block.getWidth();z++){


					for(w=1;w<block.getHeight();w++){
						ins = block.get(z,w-1) + scoreInsertion(seq1.charAt(block.getxOffset()+w));			//seq1 o seq2?
						sub = block.get(z-1,w-1) + scoreSubstitution(seq1.charAt(block.getxOffset()+w),seq2.charAt(block.getyOffset()+z));
						del = block.get(z-1,w) + scoreDeletion(seq2.charAt(block.getyOffset()+z));


						// choose the greatest
						block.set(max (ins, sub, del, 0), z, w);

						if (block.get(z, w) > max_score)
						{
							// keep track of the maximum score
							max_score = block.get(z, w);
							this.max_row = block.getyOffset()+z; this.max_col = block.getxOffset()+w;	//DA TESTARE!
						}


					}

					manager.setLLLine(block.get(z, block.getHeight()-1), z);

				}
				
				manager.writeOnHDFS(block);
				System.out.println("computed block number "+block.getId());

				block=null;
				temp=null;
				System.gc();
				
			
				//compute other blocks on the same line
				
				for(int nb=1;nb<manager.getNblock();nb++){
						block=manager.getNextBlock();
						
						//read the upper block
						temp = manager.readFromHDFS(block.getId()-manager.getNblock());
						
						
						//get last line of the upper block
//						last_line = temp.getLastLine();
						
						//compute first element
						ins = manager.getLLLine()[0] + scoreInsertion(seq1.charAt(block.getxOffset()+0));			//seq1 o seq2?
						sub = diag_el + scoreSubstitution(seq1.charAt(block.getxOffset()+0),seq2.charAt(block.getyOffset()+0));
						del = temp.get(temp.getWidth()-1, 0) + scoreDeletion(seq2.charAt(block.getyOffset()+0));		//last_line[0]


						// choose the greatest
						block.set(max (ins, sub, del, 0), 0, 0);

						if (block.get(0, 0) > max_score)
						{
							// keep track of the maximum score
							max_score = block.get(0, 0);
							this.max_row = block.getyOffset()+0; this.max_col = block.getxOffset()+0;	//DA TESTARE!
						}
						
						
						//compute the rest of first line
						
						
						for(w=1;w<block.getHeight();w++){
							ins = block.get(0,w-1) + scoreInsertion(seq1.charAt(block.getxOffset()+w));			//seq1 o seq2?
							sub = temp.get(temp.getWidth()-1,w-1) + scoreSubstitution(seq1.charAt(block.getxOffset()+w),seq2.charAt(block.getyOffset()+0));
							del = temp.get(temp.getWidth()-1,w) + scoreDeletion(seq2.charAt(block.getyOffset()+0));				
							
						
						
						// choose the greatest
						block.set(max (ins, sub, del, 0), 0, w);

						if (block.get(0, w) > max_score)
						{
							// keep track of the maximum score
							max_score = block.get(0, w);
							this.max_row = block.getyOffset()+0; this.max_col = block.getxOffset()+w;	//DA TESTARE!
						}
						
						}
						
						
						
						
						//compute the rest of first column
						
						for(z=1;z<block.getWidth();z++){

							ins = manager.getLLLine()[z] + scoreInsertion(seq1.charAt(block.getxOffset()));
							sub = manager.getLLLine()[z-1] + scoreSubstitution(seq1.charAt(block.getxOffset()),seq2.charAt(block.getyOffset()+z));
							del = block.get(z-1,0) + scoreDeletion(seq2.charAt(block.getyOffset()+z));


							// choose the greatest
							block.set(max (ins, sub, del, 0), z, 0);

							if (block.get(z, 0) > max_score)
							{
								// keep track of the maximum score
								max_score = block.get(z, 0);
								this.max_row = block.getyOffset()+z; this.max_col = block.getxOffset()+0;	//DA TESTARE!
							}
						}
						
						//compute the rest of matrix
						for(z=1;z<block.getWidth();z++){


							for(w=1;w<block.getHeight();w++){
								ins = block.get(z,w-1) + scoreInsertion(seq1.charAt(block.getxOffset()+w));			//seq1 o seq2?
								sub = block.get(z-1,w-1) + scoreSubstitution(seq1.charAt(block.getxOffset()+w),seq2.charAt(block.getyOffset()+z));
								del = block.get(z-1,w) + scoreDeletion(seq2.charAt(block.getyOffset()+z));


								// choose the greatest
								block.set(max (ins, sub, del, 0), z, w);

								if (block.get(z, w) > max_score)
								{
									// keep track of the maximum score
									max_score = block.get(z, w);
									this.max_row = block.getyOffset()+z; this.max_col = block.getxOffset()+w;	//DA TESTARE!
								}


							}

							manager.setLLLine(block.get(z, block.getHeight()-1), z);

						}
						manager.setLLLine(block.get(0, block.getHeight()-1), 0);
						
						System.out.println("computed block number "+block.getId());

						manager.writeOnHDFS(block);
						//read element in the corner
						diag_el = temp.get(temp.getWidth()-1, temp.getHeight()-1);
						temp=null;
						block=null;
						System.gc();
						
						



				}
				
			//CORRECT ON score substitution parameters	
				
			}

			


			




		}



	/**
	 * Builds an optimal local alignment between the loaded sequences.  Before it is
	 * executed, the dynamic programming matrix must already have been computed by
	 * the <CODE>computeMatrix</CODE> method.
	 *
	 * @return an optimal local alignment between the loaded sequences
	 * @throws IncompatibleScoringSchemeException If the scoring scheme is not compatible
	 * with the loaded sequences.
	 * @throws IOException 
	 * @see #computeMatrix
	 */
	protected PairwiseAlignment buildOptimalAlignment () throws
	IncompatibleScoringSchemeException, IOException
	{
		StringBuffer gapped_seq1, score_tag_line, gapped_seq2;
		int			 r, c, max_score, sub;

		// start at the cell with maximum score
		r = this.max_row;
		c = this.max_col;

//		max_score = matrix[r][c];  see under|
//											v
//		PrintWriter pw = new PrintWriter("hdfs_coords");
		HDFSMatrixManager manager = HDFSMatrixManager.getInstance();
		int new_id;
		System.out.println("Coord of max_score ["+r+" "+c+"]");
		int id = manager.getIDByCoords(r, c);
		HDFSMatrixBlock temp = manager.readFromHDFS(id);
		
		System.out.println("id del blocco con max_row and max_col ="+id);
		int other;
		
		gapped_seq1		= new StringBuffer();
		score_tag_line	= new StringBuffer();
		gapped_seq2		= new StringBuffer();
		
		int actualElem = temp.get(r%4000,c%4000);
		max_score=actualElem;
//		System.out.println("max score = "+max_score);
		HDFSMatrixBlock block;
		
		while ((r > 0 || c > 0) && (actualElem > 0))				//(matrix[r][c]>0)
		{
			/*pw.println("["+r+"],["+c+"].");
			new_id = manager.getIDByCoords(r, c);
			if(new_id!=temp.getId()){
			temp=null;
			System.gc();
			temp=manager.readFromHDFS(new_id);
			System.out.println("new id = "+new_id);}*/
//			pw.println("["+r+"],["+c+"].");
			
			
			if (r > 0){
				if(r%4000==0)
				{	//System.out.println("c = "+c);
					//System.out.println("reading block "+(temp.getId()-1));
					block=manager.readFromHDFS(temp.getId()-manager.getNblock());
					other=block.get(block.getWidth()-1, c%4000);
					block=null;
					System.gc();
				}
				else {//System.out.println("blockid= "+temp.getId()+" first r=" +r+" c="+c+" width-height    "+temp.getWidth()+"-"+temp.getHeight());
					other = temp.get((r-1)%4000, c%4000);
				}
				
				if (actualElem == other + scoreInsertion(seq1.charAt(c)))		//matrix[r][c] == matrix[r][c-1] + scoreInsertion(seq2.charAt(c))
				{
					// insertion
					gapped_seq1.insert (0, GAP_CHARACTER);
					score_tag_line.insert (0, GAP_TAG);
					gapped_seq2.insert (0, seq2.charAt(r));

					r = r - 1;
					
					new_id = manager.getIDByCoords(r, c);
					if(new_id!=temp.getId()){
						System.out.println("new id = "+new_id);
					temp=null;
					System.gc();
					temp=manager.readFromHDFS(new_id);}
					actualElem = temp.get(r%4000, c%4000);
					// skip to the next iteration
					continue;
				}
			}

			if ((r > 0) && (c > 0))
			{
				sub = scoreSubstitution(seq2.charAt(r), seq1.charAt(c));

				if(r%4000==0 && c%4000==0){
//					System.out.println("case1    r="+r+" c="+c);
//					System.out.println("reading block "+(temp.getId()-manager.getNblock()-1));

					block = manager.readFromHDFS(temp.getId()-manager.getNblock()-1);
					other = block.get(block.getWidth()-1, block.getHeight()-1);
					block=null;
					System.gc();
					
				}else if(r%4000==0 && c%4000!=0){
//					System.out.println("case2    r="+r+" c="+c);
//					System.out.println("reading block "+(temp.getId()-manager.getNblock()));
					block = manager.readFromHDFS(temp.getId()-manager.getNblock());
//					System.out.println((c%4000)-1);
					other = block.get(block.getWidth()-1, (c-1)%4000);
//					System.out.println("ok");
					
				}else if(r%4000!=0 && c%4000==0){
//					System.out.println("case3    r="+r+" c="+c);
//					System.out.println("reading block "+(temp.getId()-1));
					block = manager.readFromHDFS(temp.getId()-1);
					other = block.get((r-1)%4000, block.getHeight()-1);
					block=null;
					System.gc();
					
				}else other = temp.get((r-1)%4000, (c-1)%4000);
				
				if (actualElem == other + sub)
				{
					// substitution
					gapped_seq1.insert (0, seq1.charAt(c));
					if (seq2.charAt(r) == seq1.charAt(c))
						if (useMatchTag())
							score_tag_line.insert (0, MATCH_TAG);
						else
							score_tag_line.insert (0, seq1.charAt(c));
					else if (sub > 0)
						score_tag_line.insert (0, APPROXIMATE_MATCH_TAG);
					else
						score_tag_line.insert (0, MISMATCH_TAG);
					gapped_seq2.insert (0, seq2.charAt(r));

					r = r - 1; c = c - 1;
					
					new_id = manager.getIDByCoords(r, c);
					if(new_id!=temp.getId()){
						System.out.println("new id = "+new_id);
					temp=null;
					System.gc();
					temp=manager.readFromHDFS(new_id);}
					actualElem = temp.get(r%4000, c%4000);
					
					// skip to the next iteration
					continue;
				}
				
				
			}

			// must be a deletion
//			System.out.println("R = "+r);
			
			gapped_seq1.insert (0, seq1.charAt(c));
			score_tag_line.insert (0, GAP_TAG);
			gapped_seq2.insert  (0,GAP_CHARACTER);

			c = c - 1;
			
			new_id = manager.getIDByCoords(r, c);
			if(new_id!=temp.getId()){
				System.out.println("new id = "+new_id);
			temp=null;
			System.gc();
			temp=manager.readFromHDFS(new_id);}
			actualElem = temp.get(r%4000, c%4000);
		
		}
		
//		pw.close();

		return new PairwiseAlignment (gapped_seq1.toString(), score_tag_line.toString(),
				gapped_seq2.toString(), max_score);
	}
		

	@Override
	protected int computeScore() throws IncompatibleScoringSchemeException {
		return 0;
	}

	/**
	 * Computes the score of the best local alignment between the two sequences using the
	 * scoring scheme previously set. This method calculates the similarity value only
	 * (doesn't build the whole matrix so the alignment cannot be recovered, however it
	 * has the advantage of requiring O(n) space only).
	 *
	 * @return the score of the best local alignment between the loaded sequences
	 * @throws IncompatibleScoringSchemeException If the scoring scheme is not compatible
	 * with the loaded sequences.
	 */
	/*protected int computeScore () throws IncompatibleScoringSchemeException
	{
		int[]	array;
		int 	rows = seq1.length()+1, cols = seq2.length()+1;
		int 	r, c, tmp, ins, del, sub, max_score;

		// keep track of the maximum score
		max_score = 0;

		if (rows <= cols)
		{
			// goes columnwise
			array = new int [rows];

			// initiate first column
			for (r = 0; r < rows; r++)
				array[r] = 0;

			// calculate the similarity matrix (keep current column only)
			for (c = 1; c < cols; c++)
			{
				// set first position to zero (tmp hold values
				// that will be later moved to the array)
				tmp = 0;

				for (r = 1; r < rows; r++)
				{
					ins = array[r] + scoreInsertion(seq2.charAt(c));
					sub = array[r-1] + scoreSubstitution(seq1.charAt(r), seq2.charAt(c));
					del = tmp + scoreDeletion(seq1.charAt(r));

					// move the temp value to the array
					array[r-1] = tmp;

					// choose the greatest (or zero if all negative)
					tmp = max (ins, sub, del, 0);

					// keep track of the maximum score
					if (tmp > max_score) max_score = tmp;
				}

				// move the temp value to the array
				array[rows - 1] = tmp;
			}
		}
		else
		{
			// goes rowwise
			array = new int [cols];

			// initiate first row
			for (c = 0; c < cols; c++)
				array[c] = 0;

			// calculate the similarity matrix (keep current row only)
			for (r = 1; r < rows; r++)
			{
				// set first position to zero (tmp hold values
				// that will be later moved to the array)
				tmp = 0;

				for (c = 1; c < cols; c++)
				{
					ins = tmp + scoreInsertion(seq2.charAt(c));
					sub = array[c-1] + scoreSubstitution(seq1.charAt(r), seq2.charAt(c));
					del = array[c] + scoreDeletion(seq1.charAt(r));

					// move the temp value to the array
					array[c-1] = tmp;

					// choose the greatest (or zero if all negative)
					tmp = max (ins, sub, del, 0);

					// keep track of the maximum score
					if (tmp > max_score) max_score = tmp;
				}

				// move the temp value to the array
				array[cols - 1] = tmp;
			}
		}

		return max_score;
	}

	public void printSeq1(){
		System.out.println(seq1.toString());
	}*/
}
