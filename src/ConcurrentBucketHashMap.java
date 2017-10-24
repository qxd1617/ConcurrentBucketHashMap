import java.util.* ;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/*
 * A concurrentBucketHashMap modified from a SynchronizedBucketHashMap.
 * @Author Owen Quan Dong
 */

public class ConcurrentBucketHashMap<K, V> {
    final int numberOfBuckets ;
    final List<Bucket<K, V>> buckets ;



    /*
     * Immutable Pairs of keys and values. Immutability means
     * we don't have to worry about the key or value changing
     * under our feet. However, when the mapping for a given key
     * changes, we need to create a new Pair object.
     *
     * This is a pure data holder class.
     */
    class Pair<K, V> {
        final K key ;
        final V value ;

        Pair(K key, V value) {
            this.key = key ;
            this.value = value ;
        }
    }

    /*
     * A Bucket holds all the key/value pairs in the map that have
     * the same hash code (modulo the number of buckets). The
     * object consists of an extensible "contents" list protected
     * with a ReadWriteLock "rwl".
     */
    class Bucket<K, V> {
        private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
        private final List<Pair<K, V>> contents =
                new ArrayList<Pair<K, V>>() ;

        private void unlockRead(){
            rwl.readLock().unlock();
        }
        private void unlockWrite(){
            rwl.writeLock().unlock();
        }
        private void LockRead(){
            rwl.readLock().lock();
        }
        private void LockWrite(){
            rwl.writeLock().lock();
        }


        /*
         * Return the current Bucket size.
         */
        int size() {
            return contents.size() ;
        }

        /*
         * Get the Pair at location 'i' in the Bucket.
         */
        Pair<K, V> getPair(int i) {
            return contents.get(i) ;
        }

        /*
         * Replace the Pair at location 'i' in the Bucket.
         */
        void putPair(int i, Pair<K, V> pair) {
            contents.set(i, pair) ;
        }

        /*
         * Add a Pair to the Bucket.
         */
        void addPair(Pair<K, V> pair) {
            contents.add(pair) ;
        }

        /*
         * Remove a Pair from the Bucket by position.
         */
        void removePair(int index) {
            contents.remove(index) ;
        }
    }

    /*
     * Constructor for the SynchronizedBucketHashMap proper.
     */
    public ConcurrentBucketHashMap(int nbuckets) {
        numberOfBuckets = nbuckets ;
        buckets = new ArrayList<Bucket<K, V>>(nbuckets) ;

        for ( int i = 0 ; i < nbuckets ; i++ ) {
            buckets.add(new Bucket<K, V>()) ;
        }
    }

    /*
     * Does the map contain an entry for the specified
     * key?
     */
    public boolean containsKey(K key) {
        Bucket<K, V> theBucket = buckets.get(bucketIndex(key)) ;
        boolean contains = false;

        theBucket.LockRead();
        try {
            contains = findPairByKey(key, theBucket) >= 0;
        }
        finally{
            theBucket.unlockRead();
        }

        return contains ;
    }

    /*
     * How many pairs are in the map?
     */
    public int size() {
        int size = 0 ;
        //Loop through all the buckets and LockRead() them.
        for ( int i = 0 ; i < numberOfBuckets ; i++ ) {
            Bucket<K, V> theBucket = buckets.get(i);
            try {
                theBucket.LockRead();
            } catch (Exception e) {

            }
        }
        //Loop through all the buckets and add their size, and then unlock them.
        for ( int i = 0 ; i < numberOfBuckets ; i++ ) {
            Bucket<K, V> theBucket = buckets.get(i);
            size += theBucket.size();
            theBucket.unlockRead();
        }

        return size ;
    }

    /*
     * Return the value associated with the given Key.
     * Returns null if the key is unmapped.
     */
    public V get(K key) {
        Bucket<K, V> theBucket = buckets.get(bucketIndex(key)) ;
        Pair<K, V>   pair      = null ;
        theBucket.LockRead();
        try{
            int index = findPairByKey(key, theBucket);
            if (index >= 0){
                pair = theBucket.getPair(index);
            }

        }
        finally {
            theBucket.unlockRead();
        }

        return (pair == null) ? null : pair.value ;
    }

    /*
     * Associates the given value with the key in the
     * map, returning the previously associated value
     * (or none if the key was not previously mapped).
     */
    public V put(K key, V value) {
        Bucket<K, V> theBucket = buckets.get(bucketIndex(key)) ;
        Pair<K, V>   newPair   = new Pair<K, V>(key, value) ;
        V oldValue = null;
        theBucket.LockWrite();
        try{
            int index = findPairByKey(key, theBucket) ;
            if (index >= 0){
                Pair<K, V> pair = theBucket.getPair(index) ;

                theBucket.putPair(index, newPair) ;
                oldValue = pair.value ;
            } else {
                theBucket.addPair(newPair) ;
                oldValue = null ;
            }
        }
        finally {
            theBucket.unlockWrite();
        }
        return oldValue ;
    }

    /*
     * Remove the mapping for the given key from the map, returning
     * the currently mapped value (or null if the key is not in
     * the map.
     */
    public V remove(K key) {
        Bucket<K, V> theBucket = buckets.get(bucketIndex(key)) ;
        V removedValue = null ;
        theBucket.unlockWrite();

        try {
            int index = findPairByKey(key, theBucket);

            if (index >= 0) {
                Pair<K, V> pair = theBucket.getPair(index);

                theBucket.removePair(index);
                removedValue = pair.value;
            }
        }
        finally {
            theBucket.unlockWrite();
        }

        return removedValue ;
    }

    /****** PRIVATE METHODS ******/

    /*
     * Given a key, return the index of the Bucket
     * where the key should reside.
     */
    private int bucketIndex(K key) {
        return key.hashCode() % numberOfBuckets ;
    }

    /*
     * Find a Pair<K, V> for the given key in the given Bucket,
     * returnning the pair's index in the Bucket (or -1 if
     * unfound).
     *
     * Assumes the lock for the Bucket has been acquired.
     */
    private int findPairByKey(K key, Bucket<K, V> theBucket) {
        int size = theBucket.size() ;

        for ( int i = 0 ; i < size ; ++i ) {
            Pair<K, V> pair = theBucket.getPair(i) ;

            if ( key.equals(pair.key) ) {
                return i ;
            }
        }

        return (-1) ;
    }
}