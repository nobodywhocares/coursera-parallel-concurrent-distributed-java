package edu.coursera.parallel;

import java.util.concurrent.Phaser;

/**
 * Wrapper class for implementing one-dimensional iterative averaging using
 * phasers.
 */
public final class OneDimAveragingPhaser {

    /**
     * Default constructor.
     */
    private OneDimAveragingPhaser() {
    }

    /**
     * Sequential implementation of one-dimensional iterative averaging.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     * iterative averaging problem
     * @param n The size of this problem
     */
    public static void runSequential(final int iterations, final double[] myNew,
            final double[] myVal, final int n) {
        double[] next = myNew;
        double[] curr = myVal;

        for (int iter = 0; iter < iterations; iter++) {
            for (int j = 1; j <= n; j++) {
                next[j] = (curr[j - 1] + curr[j + 1]) / 2.0;
            }
            double[] tmp = curr;
            curr = next;
            next = tmp;
        }
    }

    /**
     * An example parallel implementation of one-dimensional iterative averaging
     * that uses phasers as a simple barrier (arriveAndAwaitAdvance).
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     * iterative averaging problem
     * @param n The size of this problem
     * @param taskCnt The number of threads/tasks to use to compute the solution
     */
    public static void runParallelBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int taskCnt) {
        Phaser ph = new Phaser(0);
        ph.bulkRegister(taskCnt);

        Thread[] threads = new Thread[taskCnt];

        for (int ii = 0; ii < taskCnt; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                final int chunkSize = (n + taskCnt - 1) / taskCnt;
                final int left = (i * chunkSize) + 1;
                int right = (left + chunkSize) - 1;
                if (right > n) {
                    right = n;
                }

                for (int iter = 0; iter < iterations; iter++) {
                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                                + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    ph.arriveAndAwaitAdvance();

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < taskCnt; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A parallel implementation of one-dimensional iterative averaging that
     * uses the Phaser.arrive and Phaser.awaitAdvance APIs to overlap
     * computation with barrier completion.
     *
     * TODO Complete this method based on the provided runSequential and
     * runParallelBarrier methods.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     * iterative averaging problem
     * @param n The size of this problem
     * @param taskCnt The number of threads/tasks to use to compute the solution
     */
    public static void runParallelFuzzyBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int taskCnt) {
        
        Phaser[] ph = new Phaser[taskCnt];
        for (int i = 0; i < ph.length; i++) {
            ph[i] = new Phaser(1);
        }
        
        Thread[] threads = new Thread[taskCnt];

        for (int ii = 0; ii < taskCnt; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                for (int iter = 0; iter < iterations; iter++) {

                    // Due to split (fuzzy) paradigm computational logic
                    // can be simplified replacing "chunk" calc overhead
                    final int left = i * (n / taskCnt) + 1;
                    final int right = (i + 1) * (n / taskCnt);

//                    final int chunkSize = (n + taskCnt - 1) / taskCnt;
//                    final int left = (i * chunkSize) + 1;
//                    int right = (left + chunkSize) - 1;
//                    if (right > n) {
//                        right = n;
//                    }

                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                                + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    
                    
                    // Notify adjacent threads they can proceed because this
                    // thread has "arrived" at the related phase.
                    // Refer to http://blog.bytecode.tech/java-phasers-made-simple/
                    // "This is called Phasers with split-phase barrier or fuzzy barrier"
                    int arrivedPhase = ph[i].arrive();
                    if (0 <= i-1) {
                        ph[i-1].awaitAdvance(arrivedPhase);
                    }
                    if (taskCnt > i+1) {
                        ph[i+1].awaitAdvance(arrivedPhase);
                    }

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < taskCnt; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
