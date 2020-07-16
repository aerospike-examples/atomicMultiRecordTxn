package com.aerospike.txnSupport;

import com.aerospike.client.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.PredExp;

import java.util.*;
import java.util.logging.Logger;

public class AerospikeClientWithTxnSupport extends AerospikeClient implements TxnSupport {
    /**
     * Member variables
     */
    private String transactionNamespace;
    private WritePolicy txnWritePolicy;
    private Policy txnReadPolicy;
    private BatchPolicy txnBatchReadPolicy;
    private WritePolicy lockPolicy;

    // If using Enterprise, we make use of durable deletes
    private boolean isEnterprise = true;

    /**
     * Static variables to support persistence of locks and transactions
     */
    public static final String PREVIOUS_RECORD_VERSION_BIN_NAME = "prevRecordVsn";
    public static final String TXN_ID_BIN_NAME = "txnID";
    public static final String TIMESTAMP_BIN_NAME = "timestamp";
    public static final String NAMESPACE_BIN_NAME = "ns";
    public static final String SET_NAME_BIN_NAME = "set";
    public static final String RECORD_KEY_BIN_NAME = "userKey";
    public static final String LOCK_TYPE = "lock";
    public static final String TXN_TYPE = "txn";

    public static final String RECORD_KEY_SET_DELIMITER = "::";

    /**
     *     Use UUIDS for lock / transaction set names to avoid namespace collisions
     */
    public static final String TRANSACTION_SET = "5b3adebd60384ebcb1ee7cdd80ab7845";
    public static final String LOCK_SET = "1d6bc26c6ca74d35a61b129be35bb24a";


    private final static Logger LOGGER = Logger.getLogger(AerospikeClientWithTxnSupport.class.getName());
    /**
     * Constructor for our transaction supporting AerospikeClient
     * Arguments are as per AerospikeClient
     * @param clientPolicy ClientPolicy object to be used when creating client. See AerospikeClient constructor for more details
     * @param host String representing host
     * @param port Aerospike service port for host
     * @param transactionNamespace namespace to be used for lock and transaction records
     */
    public AerospikeClientWithTxnSupport(ClientPolicy clientPolicy, String host, int port, String transactionNamespace){
        super(clientPolicy,host,port);
        this.transactionNamespace = transactionNamespace;

        if(clientPolicy.writePolicyDefault != null) setTxnWritePolicy(clientPolicy.writePolicyDefault);
        else setTxnWritePolicy(new WritePolicy());

        if(clientPolicy.readPolicyDefault != null) setTxnReadPolicy(clientPolicy.readPolicyDefault);
        else setTxnReadPolicy(new Policy());
    }

    /**
     * getter for transaction namespace
     * @return transaction namespace
     */
    public String getTransactionNamespace() {
        return transactionNamespace;
    }

    /**
     * getter for txn write policy
     * @return WritePolicy used for transaction records and locks
     */
    public WritePolicy getTxnWritePolicy() {
        return txnWritePolicy;
    }

    /**
     * getter for txn read policy
     * @return ReadPolicy used for transaction records and locks
     */
    public Policy getTxnReadPolicy() {
        return txnReadPolicy;
    }

    /**
     * Set policy used for creating transaction records and locks
     * @param txnWritePolicy WritePolicy
     */
    public void setTxnWritePolicy(WritePolicy txnWritePolicy) {
        if(!txnWritePolicy.durableDelete){
            if(isEnterprise) {
                LOGGER.info("Enabling durable deletes for all lock/txn deletes");
                txnWritePolicy.durableDelete = true;
            }
            else{
                LOGGER.warning("You are using this code in Community mode. Durable deletes not used");
                LOGGER.warning("But be warned, this makes the code unsafe as lock and txn records can be resurrected on cold start");
                LOGGER.warning("See https://aerospike.com/docs/guide/durable_deletes.html");
            }
        }
        else{
            // This is OK
            if(isEnterprise){}
            else{
                txnWritePolicy.durableDelete = false;
                LOGGER.warning("Setting durable delete to false as you are running in Community mode");
            }
        }
        this.txnWritePolicy = txnWritePolicy;
        lockPolicy = new WritePolicy(txnWritePolicy);
        lockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    }

    /**
     * If using Community Aerospike you should invoke setEnterprise(false)
     * Note that this will disable durable deletes making this code unreliable as deleted records may be resurrected on cold-start
     * Using this code with Community is not therefore recommended, but disabling durable deletes supplied for informational purposes
     *
     * @param enterprise - boolean determining whether txn support should run in Enterprise mode
     */
    public void setEnterprise(boolean enterprise) {
        if(!enterprise) {
            LOGGER.warning("When Enterprise set to true, durable deletes are used for all deletes");
            LOGGER.warning("Durable deletes are not available if using Community Aerospike");
            LOGGER.warning("This code will run without durable deletes if enterprise == false");
            LOGGER.warning("But be warned, locks and transaction records can be resurrected in the event of a cold start");
            LOGGER.warning("See https://aerospike.com/docs/guide/durable_deletes.html");
        }
        isEnterprise = enterprise;
        setTxnWritePolicy(txnWritePolicy);
    }

    /**
     * set policy used for reading transaction records and locks
     * @param txnReadPolicy policy used for reading locks and txn records
     */
    public void setTxnReadPolicy(Policy txnReadPolicy) {
        this.txnReadPolicy = txnReadPolicy;
        this.txnBatchReadPolicy = new BatchPolicy(txnReadPolicy);
    }

    /**
     * Save a number of records as an atomic transaction
     * This update will be treated as being part of the transaction identified by txnID for rollback and locking purposes
     * LockAcquireException will be thrown if exclusive locks cannot be taken out on all objects in the txn*
     * Objects are exclusively locked for the duration of the transaction to avoid race conditions
     * Previous state of the objects is stored to allow rollback
     * Locks are removed after txn is complete
     *
     * @param writePolicy - Write Policy to be used when updating records
     * @param recordsForUpdate - records supplied as a hash in Key:Bin[] form
     * @param generationCheckMap - key:int map allowing generationCheckMap check - useful as a check if working in environment where non-transactional writes are occurring
     * @param txnID - transaction id
     * @throws LockAcquireException if records updated by txn are already locked
     */
    @Override
    public final void put(WritePolicy writePolicy, HashMap<Key,Bin[]> recordsForUpdate, HashMap<Key,Integer> generationCheckMap, String txnID)
            throws LockAcquireException, GenFailException{
        Iterator<Key> txnKeys = recordsForUpdate.keySet().iterator();
        // Lock all timedOutTxnIDs being updated
        try {
            while (txnKeys.hasNext()) {
                createLock(txnKeys.next(), txnID);
            }
        }
        // If a lock exception arises, unlock timedOutTxnIDs
        catch(LockAcquireException e){
            removeLocksForKeys(recordsForUpdate.keySet().iterator(),txnID);
            throw e;
        }

        // Store previous versions of timedOutTxnIDs

        // Get the keys in a usable form
        Key[] keyArray = recordsForUpdate.keySet().toArray(new Key[recordsForUpdate.size()]);

        // Then store versions of objects in a hash - using keys comprised of setName and userKey, concatenated using '::' delimiter
        HashMap<String,Map<String,Object>> txnRecords;
        txnRecords = existingVersionsOfRecordsForUpdate(keyArray, txnID);

        createTransactionRecord(transactionNamespace,txnRecords,txnID);

        // Now do the transaction itself
        Key key = null;
        try {
            txnKeys = recordsForUpdate.keySet().iterator();
            // Update the individual objects
            while (txnKeys.hasNext()) {
                key = txnKeys.next();
                if(generationCheckMap.get(key) != null) {
                    writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    writePolicy.generation = generationCheckMap.get(key).intValue();
                }
                else{
                    writePolicy.generationPolicy = GenerationPolicy.NONE;
                    writePolicy.generation = 0;
                }
                if(recordsForUpdate.get(key) != null)
                    put(writePolicy, key, recordsForUpdate.get(key));
                else {
                    if (isEnterprise) writePolicy.durableDelete = true;
                    LOGGER.warning("Warning - non durable delete taking place as isEnterprise = false");
                    delete(writePolicy, key);
                }
            }
            postCommitRollbackTidy(recordsForUpdate.keySet().iterator(),txnID);
        }
        // If there is an error
        catch(AerospikeException e){
            rollback(transactionNamespace,txnRecords,txnID);
            if(e.getResultCode() == ResultCode.GENERATION_ERROR ){
                throw(new GenFailException(key,txnID));
            }
            else {
                throw (new TxnException(txnID,e));
            }
        }
    }

    /**
     * Save a number of records as an atomic transaction
     * This update will be treated as being part of the transaction identified by txnID for rollback and locking purposes
     * LockAcquireException will be thrown if exclusive locks cannot be taken out on all objects in the txn*
     * Objects are exclusively locked for the duration of the transaction to avoid race conditions
     * Previous state of the objects is stored to allow rollback
     * Locks are removed after txn is complete
     *
     * @param writePolicy - Write Policy to be used when updating records
     * @param recordsForUpdate - records supplied as a hash in Key:Bin[] form
     * @param txnID - transaction id
     * @throws LockAcquireException if records updated by txn are already locked
     */
    @Override
    public final void put(WritePolicy writePolicy, HashMap<Key,Bin[]> recordsForUpdate, String txnID) throws LockAcquireException {
        try {
            put(writePolicy, recordsForUpdate, new HashMap<Key, Integer>(), txnID);
        }
        // Shouldn't get gen fail with this version of put
        catch(GenFailException e){}
    }

    /**
     *
     * Save a number of records as an atomic transaction - see 'put' description above for full details
     * This function identical except update will be treated as being a stand-alone transaction for commit / rollback and locking purposes
     * rather than potentially being part of a larger transaction
     *
     * @param writePolicy - Write Policy to be used when updating records
     * @param recordsForUpdate - records supplied as a hash in Key:Bin[] form
     * @param generationCheckMap - key:int map allowing generationCheckMap check - useful as a check if working in environment where non-transactional writes are occurring
     * @throws LockAcquireException if records updated by txn are already locked
     * @throws GenFailException if current record generation of updated records does not match expected generation
     */
    @Override
    public final void put(WritePolicy writePolicy, HashMap<Key,Bin[]> recordsForUpdate, HashMap<Key,Integer> generationCheckMap)
            throws LockAcquireException, GenFailException{
        String txnID = TxnSupport.uniqueTxnID();
        put(writePolicy,recordsForUpdate,generationCheckMap,txnID);
    }

    /**
     *
     * Save a number of records as an atomic transaction - see 'put' description above for full details
     * This function identical except update will be treated as being a stand-alone transaction for commit / rollback and locking purposes
     * rather than potentially being part of a larger transaction
     *
     * @param writePolicy - Write Policy to be used when updating records
     * @param recordsForUpdate - records supplied as a hash in Key:Bin[] form
     * @throws LockAcquireException if records updated by txn are already locked
     */
    @Override
    public final void put(WritePolicy writePolicy, HashMap<Key,Bin[]> recordsForUpdate) throws LockAcquireException{
        String txnID = TxnSupport.uniqueTxnID();
        put(writePolicy,recordsForUpdate,txnID);
    }

    /**
     * Internal utility method
     * Creates a hash-map containing existing versions of all the timedOutTxnIDs that are going to be updated
     * Keys in the hash are a concatenation of set and userKey with a special delimiter
     *
     * Package level visibility to allow white box testing
     *
     * @param keysForRecordsForUpdate
     * @param txnID
     * @return - HashMap
     */
    HashMap<String,Map<String,Object>> existingVersionsOfRecordsForUpdate(Key[] keysForRecordsForUpdate, String txnID){
        // Get current verions of these timedOutTxnIDs
        Record[] records;
        try {
            records = get(txnBatchReadPolicy, keysForRecordsForUpdate);
        }
        catch(AerospikeException ae){
            throw new TxnException(txnID,ae);
        }

        // Then store them in a hash - using keys comprised of setName and userKey, concatenated using '::' delimiter
        HashMap<String, Map<String, Object>> txnRecords = new HashMap<String, Map<String, Object>>();
        for (int i = 0; i < keysForRecordsForUpdate.length; i++) {
            String compoundKey = new KeyAsString(keysForRecordsForUpdate[i]).toString();
            // Store previous record detail if it exists
            Map<String, Object> bins = records[i] != null ? records[i].bins : null;
            txnRecords.put(compoundKey, bins);
        }
        return txnRecords;
    }

    /**
     * Create and save transaction object - contains existing object state for all objects in transaction
     * Timestamp allows reaping of hung transactions
     *
     * Package level visibility to support white box testing
     *
     * @param transactionNamespace
     * @param txnRecords - previous versions of timedOutTxnIDs
     * @param txnID - transaction ID
     */
    void createTransactionRecord(String transactionNamespace,HashMap<String, Map<String, Object>> txnRecords, String txnID){
        Bin[] txnRecordBins = new Bin[4];
        txnRecordBins[0] = new Bin(PREVIOUS_RECORD_VERSION_BIN_NAME,txnRecords);
        txnRecordBins[1] = new Bin(TXN_ID_BIN_NAME,txnID);
        txnRecordBins[2] = new Bin(TIMESTAMP_BIN_NAME,System.currentTimeMillis());
        txnRecordBins[3] = new Bin(Constants.TYPE_BIN_NAME,TXN_TYPE);

        // Store existing object state in the 'transaction' record
        try {
            put(txnWritePolicy, keyForTxnID(txnID), txnRecordBins);
        }
        catch(AerospikeException e){
            throw(new TxnException(txnID,e));
        }
    }

    /**
     * Rollback a transaction
     * 1) Replace new versions of timedOutTxnIDs with previous
     * 2) tidy up ( delete transaction record and remove locks )
     *
     * @param transactionNamespace
     * @param txnRecords - Previous versions of the timedOutTxnIDs
     * @param txnID - Transaction ID
     */
    private void rollback(String transactionNamespace, HashMap<String,Map<String,Object>> txnRecords, String txnID){
        Iterator<String> txnKeys = txnRecords.keySet().iterator();
        Vector<Key> asKeys = new Vector<Key>();
        // Rollback previous commits
        while (txnKeys.hasNext()) {
            KeyAsString key = new KeyAsString(txnKeys.next());
            Key asKey  = key.getKey();
            try {
                Map<String, Object> originalObject = txnRecords.get(key.toString());
                if (originalObject != null)
                    put(txnWritePolicy, asKey, mapToBins(originalObject));
                else
                    delete(txnWritePolicy, asKey);
            }
            catch(AerospikeException e){
                throw(new TxnException(txnID,e));
            }
            // Build up vector of keys for tidy step
            asKeys.addElement(asKey);
        }
        postCommitRollbackTidy(asKeys.iterator(),txnID);
    }

    /**
     * Rollback transaction identified by txnID
     *
     * Package level visibility to support use by TransactionManager
     *
     * @param txnID txn to roll back
     */
    @Override
    public void rollback(String txnID) throws LockAcquireException{
        String rollbackTxnID = TxnSupport.uniqueTxnID();
        createLock(keyForTxnID(txnID),rollbackTxnID);
        Record r = get(txnReadPolicy,keyForTxnID(txnID),txnID);
        HashMap<String,Map<String,Object>> retrievedTxnRecords =
                (HashMap<String,Map<String,Object>>)r.getMap(AerospikeClientWithTxnSupport.PREVIOUS_RECORD_VERSION_BIN_NAME);
        rollback(transactionNamespace, retrievedTxnRecords,txnID);
        removeLock(keyForTxnID(txnID),rollbackTxnID);
    }

    /**
     * Assuming rollback / commit has happened - tidy up
     * Remove record of transaction ( signifies that commit / rollback has occurred in full )
     * Remove locks
     *
     * @param keys
     * @param txnID
     */
    private void postCommitRollbackTidy(Iterator<Key> keys,String txnID){
        // Remove the transaction record - this is the 'commit'
        try {
            delete(txnWritePolicy, keyForTxnID(txnID));
        }
        catch(AerospikeException ae){
            throw new TxnException(txnID,ae);
        }
        // Remove locks
        removeLocksForKeys(keys,txnID);
    }

    /**
     * Database key for txn record
     *
     * Package level access to allow testing use
     *
     * @param txnID
     * @return Key
     */
    Key keyForTxnID(String txnID){
        return new Key(transactionNamespace,TRANSACTION_SET,txnID);
    }

    /**
     * Check whether there is an incomplete txn with this id
     * @param txnID transaction being checked
     * @return boolean
     */
    public boolean txnIncomplete(String txnID){
        return get(txnReadPolicy,keyForTxnID(txnID)) != null;
    }

    /**
     * Create a lock on they object identified by key
     * Throw an error if lock cannot be acquired
     * Lock is associated with a transaction id
     *
     * @param key Aerospike Key for record requiring lock
     * @param txnID Transaction record is being locked for
     */
    @Override
    public void createLock(Key key, String txnID) throws LockAcquireException{
        Key lockKey  = lockKey(key);

        Bin txnIDBin = new Bin(TXN_ID_BIN_NAME,txnID);
        Bin[] lockBinAsArray;
        Bin typeBin = new Bin(Constants.TYPE_BIN_NAME, LOCK_TYPE);
        Bin namespaceBin = new Bin(NAMESPACE_BIN_NAME,key.namespace);
        Bin setNameBin = new Bin(SET_NAME_BIN_NAME, key.setName);
        Bin recordKeyBin = new Bin(RECORD_KEY_BIN_NAME, key.userKey.toString());
        Bin lockTimeBin = new Bin(TIMESTAMP_BIN_NAME,System.currentTimeMillis());
        lockBinAsArray = new Bin[]{typeBin, txnIDBin, namespaceBin,setNameBin, recordKeyBin,lockTimeBin};
        try{
            // Note lockPolicy is CREATE_ONLY - see setTxnWritePolicy
            put(lockPolicy,lockKey,lockBinAsArray);
        }
        catch(AerospikeException ae){
            if(ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                Record r = get(txnReadPolicy,lockKey,txnID);
                // Throw an error if lock is held by a different txn
                if(r == null || ! r.getString(TXN_ID_BIN_NAME).equals(txnID)) throw new LockAcquireException(key, txnID);
            }
            else{
                throw new TxnException(txnID,ae);
            }
        }
    }

    /**
     * Remove lock for a given key/txnID combination
     * If combination does not exist, no action is taken
     *
     * @param key Aerospike Key for record requiring lock removal
     * @param txnID Transaction record was locked
     */
    public void removeLock(Key key,String txnID){
        Key lockKey  = lockKey(key);
        PredExp[] txnEqual = {PredExp.stringBin(TXN_ID_BIN_NAME),PredExp.stringValue(txnID),PredExp.stringEqual()};
        WritePolicy deleteLockPolicy = new WritePolicy(txnWritePolicy);
        deleteLockPolicy.predExp = txnEqual;
        try {
            delete(deleteLockPolicy, lockKey);
        }
        catch(AerospikeException ae){
            throw new TxnException(txnID,ae);
        }
    }

    /**
     * Returns a key for a lock on the object identifed by the 'key' parameter
     *
     * Package level access to allow white box testing
     *
     * @param key Aerospike Key for record whose lock is required
     * @return key to a lock object
     */
    static Key lockKey(Key key){
        return new Key(key.namespace,LOCK_SET,new KeyAsString(key).toString());
    }

    /**
     * Is there currently a lock on the object identified by 'key'
     *
     * @param key Aerospike Key for record whose lock is being checked
     * @return boolean
     */
    public boolean lockExists(Key key) {
        return get(txnReadPolicy, lockKey(key)) != null;
    }

    /**
     * Remove locks for a given list of keys associated with a given transaction id
     * @param keys - keys requiring lock removal
     * @param txnID - transaction these keys are part of
     */
    private void removeLocksForKeys(Iterator<Key> keys,String txnID){
        while (keys.hasNext()) {
            removeLock(keys.next(), txnID);
        }
    }

    private Record get(Policy p,Key key,String txnID){
        try{
            return get(p,key);
        }
        catch(AerospikeException ae){
            throw new TxnException(txnID,ae);
        }
    }
    /**
     * Utility method to turn a map into an array of bins
     * @param map
     * @return bin[]
     */
    private static Bin[] mapToBins(Map<String,Object> map){
        Bin[] bins;
        if(map != null) {
            bins = new Bin[map.size()];
            Iterator<String> i = map.keySet().iterator();
            int counter = 0;
            while (i.hasNext()) {
                String binName = i.next();
                bins[counter] = new Bin(binName, map.get(binName));
                counter++;
            }
        }
        else
            bins = null;
        return bins;
    }

    /**
     * Convert a byte array into a hexadecimal string representation
     * @param byteArray
     * @return String
     */
    static String byteArrayToString(byte[] byteArray){
        String[] s = new String[byteArray.length];
        for(int i=0;i<byteArray.length;i++){
            s[i] = String.format("%02X",Byte.toUnsignedInt(byteArray[i]));
        }
        return String.join("",s);
    }

    /**
     * Convert a string representation of a byte array to bytes
     * @param byteArrayAsString
     * @return byte[]
     */
    static byte[] stringToByteArray(String byteArrayAsString){
        if(byteArrayAsString.length() %2 != 0) throw new IllegalArgumentException("Input string must contain an even number of characters");
        byte[] bytes = new byte[byteArrayAsString.length() /2];
        for(int i=0;i<byteArrayAsString.length() / 2;i++){
            bytes[i] = (byte)Integer.parseInt(byteArrayAsString.substring(i*2,i*2+2),16);
        }
        return bytes;
    }

    /**
     * Utility class to marshal keys into strings and back again
     *
     * Used as we store keys as strings when recording txn info ( used in rollback if needed )
     *
     * Digest is serialized / de-serialized to ensure type information preserved
     */
    public static class KeyAsString{
        private String set;
        private String namespace;
        private byte[] digest;

        /**
         * Store the namespace/set/userKey components of a Key, to marshal them as a String
         * @param key
         */
        KeyAsString(Key key){
            namespace = key.namespace;
            set = key.setName;
            digest=key.digest;
        }

        /**
         * Parses a string key created via this class into its constituent parts
         *
         * @param key
         * @throws KeyFormatException
         */
        // Note namespace and set cannot contain ':' so should '::' delimiter be encountered more than twice this is tractable
        KeyAsString(String key) throws KeyFormatException{
            String[] parts = key.split(RECORD_KEY_SET_DELIMITER);

            if(parts.length < 3) throw new KeyFormatException(key);
            String lastPart = String.join("",Arrays.copyOfRange(parts,2,parts.length));
            namespace = parts[0];
            set = parts[1];
            digest = stringToByteArray(lastPart);
        }

        /**
         * Allows a Key to be turned into a string by joining the constituent parts via the '::' delimiter
         *
         * @return Key as String
         */
        public String toString(){
            return namespace + RECORD_KEY_SET_DELIMITER + set + RECORD_KEY_SET_DELIMITER + byteArrayToString(digest);
        }

        /**
         * Allows the conversion of a String created using this class into a Key object
         * @return Aerospike Key object based on data supplied to constructor
         */
        public Key getKey(){
            return new Key(namespace,digest,set,null);
        }
    }

    /**
     * Error thrown if we attempt to convert a String to a Key where it does not satisfy the required format
     *
     * Runtime exception as it should not happen
     *
     * Expected format is namespace::set::userKey
     */
    public static class KeyFormatException extends IllegalArgumentException {
        public KeyFormatException(String key) {
            super("Key " + key + " should contain three parts delimited by " + RECORD_KEY_SET_DELIMITER);
        }
    }

}
