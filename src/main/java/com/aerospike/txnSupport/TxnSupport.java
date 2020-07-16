package com.aerospike.txnSupport;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.txnSupport.AerospikeClientWithTxnSupport;

import java.util.HashMap;
import java.util.UUID;

public interface TxnSupport {
    /**
     * Support atomic put of multiple records - supplied as a HashMap
     * @param policy - write policy
     * @param recordsForUpdate - HashMap of records for update
     * @param txnID - if part of a larger transaction
     * @throws LockAcquireException if records updated by txn are already locked
     */
    void put(WritePolicy policy, HashMap<Key,Bin[]> recordsForUpdate, String txnID) throws LockAcquireException;

    /**
     * Support atomic put of multiple records - supplied as a HashMap
     * Additionally supports generation check where we non-transactional updates may be taking place
     *
     * @param policy - write policy
     * @param recordsForUpdate - HashMap of records for update
     * @param generationMap - key:int map allowing generation check - useful as a check if working in environment where non-transactional writes are occurring
     * @param txnID - if part of a larger transaction
     * @throws LockAcquireException if records updated by txn are already locked
     * @throws GenFailException if current record generation of updated records does not match expected generation
     */
    void put(WritePolicy policy, HashMap<Key,Bin[]> recordsForUpdate, HashMap<Key,Integer> generationMap, String txnID)
            throws LockAcquireException, GenFailException;

    /**
     * As above but without txnID parameter - this transaction is self-contained and atomic
     *
     * @param policy - write policy
     * @param recordsForUpdate - HashMap of records for update
     * @param generationMap - key:int map allowing generation check - useful as a check if working in environment where non-transactional writes are occurring
     * @throws LockAcquireException if records updated by txn are already locked
     * @throws GenFailException if current record generation of updated records does not match expected generation
     */
    void put(WritePolicy policy, HashMap<Key,Bin[]> recordsForUpdate, HashMap<Key,Integer> generationMap) throws LockAcquireException, GenFailException;

    /**
     * As above but without txnID parameter - this transaction is self-contained and atomic
     * @param policy - write policy
     * @param recordsForUpdate - HashMap of records for update
     * @throws LockAcquireException if records updated by txn are already locked
     */
    void put(WritePolicy policy, HashMap<Key,Bin[]> recordsForUpdate) throws LockAcquireException;

    /**
     * Rollback a transaction identified by txnID
     *
     * @param txnID txn to roll back
     */
    void rollback(String txnID) throws LockAcquireException;

    /**
     * Create a lock on they object identified by key
     * Throw an error if lock cannot be acquired
     * Lock is associated with a transaction id
     *
     * @param key Aerospike Key for record requiring lock
     * @param txnID Transaction record is being locked for
     * @throws LockAcquireException if records updated by txn are already locked
     */
    void createLock(Key key, String txnID) throws LockAcquireException;

    /**
     * Remove lock for a given key/txnID combination
     * If combination does not exist, no action is taken
     *
     * @param key Aerospike Key for record requiring lock removal
     * @param txnID Transaction record was locked
     */
    void removeLock(Key key,String txnID);

    /**
     * Is there currently a lock on the object identified by 'key'
     *
     * @param key Aerospike Key for record whose lock is being checked
     * @return boolean
     */
    boolean lockExists(Key key);

    /**
     * Utility method to get string uuid
     *
     * @return randomly generated uuid as string
     */
    static String uniqueTxnID(){
        return UUID.randomUUID().toString();
    }

    /**
     * Check whether there is an incomplete txn with this id
     * @param txnID transaction being checked
     * @return boolean
     */
    boolean txnIncomplete(String txnID);


    /**
     * Error thrown when a Key cannot be locked due to it being locked already
     *
     * The id of the locking txn and the object Key are available via the Exception object
     */
    class LockAcquireException extends Exception {

        private Key key;
        private String txnID;

        public LockAcquireException(Key key, String txnID)
        {
            super("Lock already exists for key "+key.toString()+ "for txn id "+txnID);
            this.key = key;
            this.txnID = txnID;

        }

        public Key getKey() {
            return key;
        }

        public String getTxnID() {
            return txnID;
        }

    }

    /**
     * Error thrown when a Key cannot be locked due to it being locked already
     *
     * The id of the locking txn and the object Key are available via the Exception object
     */
    class GenFailException extends Exception {

        private Key key;
        private String txnID;

        public GenFailException(Key key, String txnID)
        {
            super("Generation check failed for key "+key.toString()+ "for txn id "+txnID);
            this.key = key;
            this.txnID = txnID;

        }

        public Key getKey() {
            return key;
        }

        public String getTxnID() {
            return txnID;
        }

    }

    /**
     * Adds transaction id to any untrapped Aerospike Exceptions
     */
    class TxnException extends AerospikeException {

        private String txnID;

        public TxnException(String txnID, AerospikeException e)
        {
            super("Transaction "+txnID+" failed",e);
            this.txnID = txnID;
            this.resultCode = e.getResultCode();
        }

        public String getTxnID() {
            return txnID;
        }

    }

}
