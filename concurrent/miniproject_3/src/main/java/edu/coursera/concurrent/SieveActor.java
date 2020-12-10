package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import static edu.rice.pcdp.PCDP.finish;
import java.util.ArrayList;
import java.util.List;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determine the number of primes <= limit.
 */
public final class SieveActor extends Sieve {

    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {

        // Seed first actor w/ first prime "2" as in the sequential alg
        final SieveActorActor sieveActor = new SieveActorActor(2);
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                sieveActor.send(i);
            }
            sieveActor.send(0);
        });

        // Sum up the number of local primes from each actor in the chain.
        int primeCntTotal = 0;
        SieveActorActor currentActor = sieveActor;
        while (null != currentActor) {
            primeCntTotal += currentActor.getPrimeCount();
            currentActor = currentActor.getNextActor();
        }
        return primeCntTotal;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {

        static final int PRIME_CNT_MAX = 512;
        static final Integer TERMINATE_MSG = 0;

        List<Integer> primes = new ArrayList<>(PRIME_CNT_MAX);

        SieveActorActor nextActor = null;

        public SieveActorActor() {
            this.nextActor = null;
        }

        public SieveActorActor(final Integer initialPrime) {
            this();
            if (null != initialPrime) {
                this.primes.add(initialPrime);
            }
        }

        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {

            assert msg instanceof Integer;
            Integer candidate = (Integer) msg;

            if (!TERMINATE_MSG.equals(candidate)) {
                if (isPrime(candidate)) {
                    if (isFull()) {
                        // This actor is at capacity so activate next actor 
                        // in pipeline
                        if (null == this.nextActor) {
                            this.nextActor = new SieveActorActor();
                        }
                        nextActor.send(msg);
                    } else {
                        // Add prime locally because not yet at capacity
                        this.primes.add(candidate);
                    }
                }
            }
        }

        public boolean isPrime(Integer candidate) {
            return primes.parallelStream().noneMatch(prime -> candidate % prime == 0);
        }

        public int getPrimeCount() {
            return this.primes.size();
        }

        public boolean isFull() {
            return PRIME_CNT_MAX < getPrimeCount();
        }

        public SieveActorActor getNextActor() {
            return this.nextActor;
        }
    }

    public static void main(String[] args) {
        try {
            
            ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
            SieveActor.class.getClassLoader().setDefaultAssertionStatus(true);
            Sieve actor = new SieveActor();
            int primeCnt = actor.countPrimes(10);
            System.out.println("PRIM CNT 10 LIMIT: "+primeCnt);
            assert 5 == primeCnt;
            primeCnt = actor.countPrimes(100);
            System.out.println("PRIM CNT 100 LIMIT: "+primeCnt);
            assert 25 == primeCnt;
            primeCnt = actor.countPrimes(1000);
            System.out.println("PRIM CNT 1000 LIMIT: "+primeCnt);
            assert 168 == primeCnt;
            primeCnt = actor.countPrimes(2500);
            System.out.println("PRIM CNT 2500 LIMIT: "+primeCnt);
            primeCnt = actor.countPrimes(10000);
            System.out.println("PRIM CNT 10000 LIMIT: "+primeCnt);
            primeCnt = actor.countPrimes(100000);
            System.out.println("PRIM CNT 100000 LIMIT: "+primeCnt);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(0);
        }
    }
}
