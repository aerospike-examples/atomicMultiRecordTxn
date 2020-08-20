package com.aerospike.txnSupport;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

/**
 * This class is responsible for rollback of expired transactions
 * and release of expired locks
 */
public class TransactionManager {
    /**
     * Class member variables
     */
    private final AerospikeClientWithTxnSupport client;
    private int transactionTimeOutMillis = DEFAULT_TXN_EXPIRY_PERIOD_MILLIS;
    private final Policy indexCreateCheckReadPolicy;
    private final WritePolicy writePolicy;
    private final QueryPolicy queryPolicy =  new QueryPolicy();

    /**
     * Default values
     */
    public static final int DEFAULT_TXN_EXPIRY_PERIOD_MILLIS = 30000;

    /**
     * Implementation detail - we maintain a record of whether indices have been created
     */
    private static final String INDEX_CREATED_TYPE = "index-created-record";

    private final String lockIndexCreatedKey = "62d020b4-7561-410d-a269-16bc32194409";
    private final String txnIndexCreatedKey = "c86304ee-a0fa-48ef-86b2-21040979d0a2";

    public final static String INDEX_CREATION_RECORD_SET = "index-created";

    private static final String VALUE_BIN_NAME = "value";


    /**
     * TransactionManager constructor
     * @param client requires an AerospikeClientWithTxnSupport object
     */
    public TransactionManager(AerospikeClientWithTxnSupport client){
        this.client = client;
        indexCreateCheckReadPolicy = client.getTxnReadPolicy();
        writePolicy = client.getTxnWritePolicy();
        setup();
    }

    /**
     * Get transaction timeout
     * @return txn timeout in ms (int)
     */
    public int getTransactionTimeOutMillis() {
        return transactionTimeOutMillis;
    }

    /**
     * Set transaction timeout
     * @param transactionTimeOutMillis txn timeout in ms
     */
    public void setTransactionTimeOutMillis(int transactionTimeOutMillis) {
        this.transactionTimeOutMillis = transactionTimeOutMillis;
    }

    /**
     * Rollback expired transactions ( those with a timestamp lt NOW - time out )
     * @return count of rolled back transactions
     */
    public int rollbackExpiredTxns() throws TxnSupport.LockAcquireException {
        int rolledBackTxns = 0;
        for (String s : getExpiredTxnIDs()) {
            client.rollback(s);
            rolledBackTxns++;
        }
        return rolledBackTxns;
    }



    /**
     * Remove all timed out orphan locks ( those not associated with an existing txn record )
     * @return count of removed locks
     */
    public int removeOrphanLocks(){
        int orphanLocks = 0;
        // Get list of all current txns and put it in a hash
        Statement txnStmt = new Statement();
        txnStmt.setNamespace(client.getTransactionNamespace());
        txnStmt.setSetName(AerospikeClientWithTxnSupport.TRANSACTION_SET);
        txnStmt.setBinNames(AerospikeClientWithTxnSupport.TXN_ID_BIN_NAME);
        txnStmt.setFilter(Filter.equal(Constants.TYPE_BIN_NAME,AerospikeClientWithTxnSupport.TXN_TYPE));

        Iterator<KeyRecord> txnRecords = client.query(queryPolicy, txnStmt).iterator();
        HashMap<String,Integer> txnHash = new HashMap<String,Integer>();
        while(txnRecords.hasNext()){
            txnHash.put(txnRecords.next().record.getString(AerospikeClientWithTxnSupport.TXN_ID_BIN_NAME),1);
        }

        // Get all locks
        Statement stmt = new Statement();
        stmt.setNamespace(client.getTransactionNamespace());
        stmt.setSetName(AerospikeClientWithTxnSupport.LOCK_SET);
        stmt.setFilter(Filter.equal(Constants.TYPE_BIN_NAME,AerospikeClientWithTxnSupport.LOCK_TYPE));
        stmt.setPredExp(PredExp.integerBin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME),PredExp.integerValue(System.currentTimeMillis() - transactionTimeOutMillis),PredExp.integerLess());

        // If the txn they are associated with does not exist remove them
        for (KeyRecord r : client.query(queryPolicy, stmt)) {
            String txnID = r.record.getString(AerospikeClientWithTxnSupport.TXN_ID_BIN_NAME);
            if (txnHash.get(txnID) == null) {
                client.delete(writePolicy, r.key);
                orphanLocks++;
            }
        }
        return orphanLocks;
    }

    /**
     * Get a list of all expired transaction ids
     * @return
     */
    private Vector<String> getExpiredTxnIDs(){
        Vector<String> txnIDList = new Vector<String>();
        // Get list of all current txns and put it in a hash
        Statement txnStmt = new Statement();
        txnStmt.setNamespace(client.getTransactionNamespace());
        txnStmt.setSetName(AerospikeClientWithTxnSupport.TRANSACTION_SET);
        txnStmt.setBinNames(AerospikeClientWithTxnSupport.TXN_ID_BIN_NAME);
        txnStmt.setFilter(Filter.equal(Constants.TYPE_BIN_NAME,AerospikeClientWithTxnSupport.TXN_TYPE));
        txnStmt.setPredExp(PredExp.integerBin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME),PredExp.integerValue(System.currentTimeMillis() - transactionTimeOutMillis),PredExp.integerLess());

        for (KeyRecord keyRecord : client.query(queryPolicy, txnStmt)) {
            txnIDList.addElement(keyRecord.record.getString(AerospikeClientWithTxnSupport.TXN_ID_BIN_NAME));
        }
        return txnIDList;
    }


    /**
     * We need indices to find lock and txn records
     * Set these up below and create records to indicate that they have been set up
     */
    public void setup(){
        // Create index on type=lock
        try {
            if (client.get(indexCreateCheckReadPolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, lockIndexCreatedKey)) == null) {
                IndexTask task = client.createIndex(indexCreateCheckReadPolicy, client.getTransactionNamespace(), AerospikeClientWithTxnSupport.LOCK_SET, AerospikeClientWithTxnSupport.LOCK_TYPE,
                        Constants.TYPE_BIN_NAME, IndexType.STRING);
                task.waitTillComplete();
                client.put(writePolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, lockIndexCreatedKey),
                        new Bin(Constants.TYPE_BIN_NAME, INDEX_CREATED_TYPE),new Bin(VALUE_BIN_NAME,AerospikeClientWithTxnSupport.LOCK_TYPE));
            }
        }
        catch(AerospikeException e){
            if(e.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                client.put(writePolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, lockIndexCreatedKey),
                        new Bin(Constants.TYPE_BIN_NAME, INDEX_CREATED_TYPE),new Bin(VALUE_BIN_NAME,AerospikeClientWithTxnSupport.LOCK_TYPE));
            }
            else{
                throw(e);
            }
        }
        // Create index on type=txn
        try {
            if (client.get(indexCreateCheckReadPolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, txnIndexCreatedKey)) == null) {
                IndexTask task = client.createIndex(indexCreateCheckReadPolicy, client.getTransactionNamespace(), AerospikeClientWithTxnSupport.TRANSACTION_SET, AerospikeClientWithTxnSupport.TXN_TYPE,
                        Constants.TYPE_BIN_NAME, IndexType.STRING);
                task.waitTillComplete();
                client.put(writePolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, txnIndexCreatedKey),
                        new Bin(Constants.TYPE_BIN_NAME, INDEX_CREATED_TYPE),new Bin(VALUE_BIN_NAME,AerospikeClientWithTxnSupport.TXN_TYPE));
            }
        }
        catch(AerospikeException e){
            if(e.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                client.put(writePolicy, new Key(client.getTransactionNamespace(), INDEX_CREATION_RECORD_SET, txnIndexCreatedKey),
                        new Bin(Constants.TYPE_BIN_NAME, INDEX_CREATED_TYPE),new Bin(VALUE_BIN_NAME,AerospikeClientWithTxnSupport.TXN_TYPE));
            }
            else{
                throw(e);
            }
        }
    }
}
