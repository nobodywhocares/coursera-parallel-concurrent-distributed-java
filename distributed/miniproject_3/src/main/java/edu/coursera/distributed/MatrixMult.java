package edu.coursera.distributed;

import edu.coursera.distributed.util.MPI;
import edu.coursera.distributed.util.MPI.MPIException;

/**
 * A wrapper class for a parallel, MPI-based matrix multiply implementation.
 */
public class MatrixMult {

    /**
     * A parallel implementation of matrix multiply using MPI to express SPMD
     * parallelism. In particular, this method should store the output of
     * multiplying the matrices a and b into the matrix c.
     *
     * This method is called simultaneously by all MPI ranks in a running MPI
     * program. For simplicity MPI_Init has already been called, and
     * MPI_Finalize should not be called in parallelMatrixMultiply.
     *
     * On entry to parallelMatrixMultiply, the following will be true of a, b,
     * and c:
     *
     * 1) The matrix a will only be filled with the input values on MPI rank
     * zero. Matrix a on all other ranks will be empty (initialized to all
     * zeros). 2) Likewise, the matrix b will only be filled with input values
     * on MPI rank zero. Matrix b on all other ranks will be empty (initialized
     * to all zeros). 3) Matrix c will be initialized to all zeros on all ranks.
     *
     * Upon returning from parallelMatrixMultiply, the following must be true:
     *
     * 1) On rank zero, matrix c must be filled with the final output of the
     * full matrix multiplication. The contents of matrix c on all other ranks
     * are ignored.
     *
     * Therefore, it is the responsibility of this method to distribute the
     * input data in a and b across all MPI ranks for maximal parallelism,
     * perform the matrix multiply in parallel, and finally collect the output
     * data in c from all ranks back to the zeroth rank. You may use any of the
     * MPI APIs provided in the mpi object to accomplish this.
     *
     * A reference sequential implementation is provided below, demonstrating
     * the use of the Matrix class's APIs.
     *
     * @param a Input matrix
     * @param b Input matrix
     * @param c Output matrix
     * @param mpi MPI object supporting MPI APIs
     * @throws MPIException On MPI error. It is not expected that your
     * implementation should throw any MPI errors during normal operation.
     */
    public static void parallelMatrixMultiply(Matrix a, Matrix b, Matrix c,
            final MPI mpi) throws MPIException {
//        for (int i = 0; i < c.getNRows(); i++) {
//            for (int j = 0; j < c.getNCols(); j++) {
//                c.set(i, j, 0.0);
//
//                for (int k = 0; k < b.getNRows(); k++) {
//                    c.incr(i, j, a.get(i, k) * b.get(k, j));
//                }
//            }
//        }
        // Determine this process' rank
        final int rank = mpi.MPI_Comm_rank(mpi.MPI_COMM_WORLD);
        // Determine process count
        final int procCnt = mpi.MPI_Comm_size(mpi.MPI_COMM_WORLD);
        // Calculate process chunks
        final int rowCnt = c.getNRows();
        final int rowChunk = (rowCnt + procCnt - 1) / procCnt;
        final int rowStart = rank * rowChunk;
        int rowEnd = (rank + 1) * rowChunk;
        if (rowEnd > rowCnt) {
            rowEnd = rowCnt;
        }
// Invocation of even static messages is slowing below 3.0 performance barrier
// to 2.9
//        broadcast(a, b, mpi);
//        updateMatrices(a, b, c, rowStart, rowEnd);
//        processRank(a, b, c, mpi, rank, procCnt, rowCnt, rowStart, rowEnd, rowChunk);

        // Convert matrix 'a' and 'b' to array of values and broadcast with ...
        // offset, cell count, root rank, and MPI communicator to other ranks
        mpi.MPI_Bcast(a.getValues(), 0, a.getNRows() * a.getNCols(), 0, mpi.MPI_COMM_WORLD);
        mpi.MPI_Bcast(b.getValues(), 0, b.getNRows() * b.getNCols(), 0, mpi.MPI_COMM_WORLD);        
        for (int i = rowStart; i < rowEnd; i++) {
            for (int j = 0; j < c.getNCols(); j++) {
                c.set(i, j, 0.0);

                for (int k = 0; k < b.getNRows(); k++) {
                    c.incr(i, j, a.get(i, k) * b.get(k, j));
                }
            }
        }        
        if (0 == rank) {
            // process/generate rank
            MPI.MPI_Request[] requests = new MPI.MPI_Request[procCnt - 1];
            for (int i = 1; i < procCnt; i++) {
                int rankStartRow = i * rowChunk;
                int rankEndRow = (i + 1) * rowChunk;
                if (rankEndRow > rowCnt) {
                    rankEndRow = rowCnt;
                }

                final int rowOffset = rankStartRow * c.getNCols();
                final int nElements = (rankEndRow - rankStartRow) * c.getNCols();

                requests[i - 1] = mpi.MPI_Irecv(c.getValues(), rowOffset, nElements, i, i, mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Waitall(requests);
        } else {
            // rank already generated so publish/send result
            mpi.MPI_Send(c.getValues(), rowStart * c.getNCols(),
                    (rowEnd - rowStart) * c.getNCols(), 0, rank,
                    mpi.MPI_COMM_WORLD);
        }

    }
    
    protected static void broadcast(Matrix a, Matrix b, final MPI mpi) throws MPIException {
        // Convert matrix 'a' and 'b' to array of values and broadcast with ...
        // offset, cell count, root rank, and MPI communicator to other ranks
        mpi.MPI_Bcast(a.getValues(), 0, a.getNRows() * a.getNCols(), 0, mpi.MPI_COMM_WORLD);
        mpi.MPI_Bcast(b.getValues(), 0, b.getNRows() * b.getNCols(), 0, mpi.MPI_COMM_WORLD);        
    }
    
    protected static void updateMatrices(
            Matrix a, Matrix b, Matrix c, 
            int rowStart, int rowEnd) {
        for (int i = rowStart; i < rowEnd; i++) {
            for (int j = 0; j < c.getNCols(); j++) {
                c.set(i, j, 0.0);

                for (int k = 0; k < b.getNRows(); k++) {
                    c.incr(i, j, a.get(i, k) * b.get(k, j));
                }
            }
        }        
    }
    
    protected static void processRank(Matrix a, Matrix b, Matrix c,
            final MPI mpi, int rank, int procCnt,
            int rowCnt, int rowStart, int rowEnd, int rowChunk) 
            throws MPIException {
        if (0 == rank) {
            // process/generate rank
            MPI.MPI_Request[] requests = new MPI.MPI_Request[procCnt - 1];
            for (int i = 1; i < procCnt; i++) {
                int rankStartRow = i * rowChunk;
                int rankEndRow = (i + 1) * rowChunk;
                if (rankEndRow > rowCnt) {
                    rankEndRow = rowCnt;
                }

                final int rowOffset = rankStartRow * c.getNCols();
                final int nElements = (rankEndRow - rankStartRow) * c.getNCols();

                requests[i - 1] = mpi.MPI_Irecv(c.getValues(), rowOffset, nElements, i, i, mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Waitall(requests);
        } else {
            // rank already generated so publish/send result
            mpi.MPI_Send(c.getValues(), rowStart * c.getNCols(),
                    (rowEnd - rowStart) * c.getNCols(), 0, rank,
                    mpi.MPI_COMM_WORLD);
        }
        
    }
}
