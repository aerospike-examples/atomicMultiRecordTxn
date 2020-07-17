package com.aerospike.txnSupport;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

public class AerospikeClientWithTxnSupportTest {

    // Client object we will use for the test
    private static ClientPolicy clientPolicy = new ClientPolicy();

    private static AerospikeClientWithTxnSupport aerospikeClientWithTxnSupport =
            new AerospikeClientWithTxnSupport(clientPolicy, TestConstants.AEROSPIKE_SERVER_IP, TestConstants.AEROSPIKE_SERVER_PORT, TestConstants.TEST_TXN_NAMESPACE);

    // Create two objects associated with keys key1 and key2
    private static Key TEST_KEY_1 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0001");
    private static Key TEST_KEY_2 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0002");
    private static Key TEST_KEY_3 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0003");
    private static Key TEST_KEY_4 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0004");
    private static Key TEST_KEY_5 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0005");
    private static Key TEST_KEY_6 = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"TEST-0006");
    
    private WritePolicy testWritePolicy = new WritePolicy();
    private Policy testReadPolicy = new Policy();

    static{
        aerospikeClientWithTxnSupport.setEnterprise(false);
    }

    /**
     * Check you can't lock the same record twice with different txn ids
     */
    @Test
    public void cantLockSameRecordTwiceWithDifferentTxnID() throws AerospikeClientWithTxnSupport.LockAcquireException {
        String testUserKey = "ABCD";
        String txnID1 = TxnSupport.uniqueTxnID();
        Key key = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,testUserKey);
        // Lock once
        aerospikeClientWithTxnSupport.createLock(key,txnID1);
        try {
            String txnID2 = TxnSupport.uniqueTxnID();
            aerospikeClientWithTxnSupport.createLock(key,txnID2);
            Assert.fail("Should throw a Lock Exception - did not - test failed");
        }
        catch(AerospikeClientWithTxnSupport.LockAcquireException lockException){
            // We want an exception of this type to be thrown, so no action if it is
        }
        catch(Exception e){
            Assert.fail("Exception "+ e.getMessage()+ " thrown - should be "+ (new AerospikeClientWithTxnSupport.LockAcquireException(key,txnID1)).getClass().getName());
        }
        finally{
            aerospikeClientWithTxnSupport.removeLock(key,txnID1);
        }
    }

    /**
     * Check you can't lock the same record twice with different txn ids
     */
    @Test
    public void canLockSameRecordTwiceWithSameTxnID() throws AerospikeClientWithTxnSupport.LockAcquireException{
        String testUserKey = "ABCD";
        String txnID = TxnSupport.uniqueTxnID();
        Key key = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,testUserKey);
        // Lock once
        aerospikeClientWithTxnSupport.createLock(key,txnID);
        try {
            aerospikeClientWithTxnSupport.createLock(key,txnID);
        }
        catch(AerospikeClientWithTxnSupport.LockAcquireException lockException){
            Assert.fail("Locking twice with same txn id should succeeed");
        }
        catch(Exception e){
            Assert.fail("Exception "+ e.getMessage()+ " thrown - should succeed");
        }
        finally{
            aerospikeClientWithTxnSupport.removeLock(key,txnID);
        }
    }

    /**
     * Check that a lock removed when the correct transaction id is supplied
     */
    @Test
    public void canRemoveLock()  throws AerospikeClientWithTxnSupport.LockAcquireException{
        // Create a lock for a fictitious key for a given txn id
        String testUserKey = "ABCD";
        String testTxnID = UUID.randomUUID().toString();
        Key key = new Key(TestConstants.TEST_NAMESPACE, TestConstants.AEROSPIKE_TEST_SET_NAME, testUserKey);
        aerospikeClientWithTxnSupport.createLock(key, testTxnID);

        // Delete lock
        aerospikeClientWithTxnSupport.removeLock(key, testTxnID);
        // Lock should no longer exist
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(key));
    }

    /**
     * Check that a lock is only removed when the correct transaction id is supplied
     */
    @Test
    public void cantRemoveLockForDifferentTransaction()  throws AerospikeClientWithTxnSupport.LockAcquireException{
        // Create a lock for a fictitious key for a given txn id
        String testUserKey = "ABCD";
        String testTxnID = UUID.randomUUID().toString();
        Key key = new Key(TestConstants.TEST_NAMESPACE, TestConstants.AEROSPIKE_TEST_SET_NAME, testUserKey);
        aerospikeClientWithTxnSupport.createLock(key, testTxnID);

        // Delete lock but supply a 'wrong' txn id
        String nonExistentTxnID = "123XYZ";
        aerospikeClientWithTxnSupport.removeLock(key, nonExistentTxnID);
        // lock should still exist
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(key));
        // Delete lock with correct txn id
        aerospikeClientWithTxnSupport.removeLock(key, testTxnID);
        // Lock should no longer exist
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(key));
    }

    /**
     * Check that if we look to update objects with keys key1/key2 then an exception is thrown
     * if one of those objects is already locked
     */
    @Test
    public void exceptionRaisedWhenRecordLocksExist() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1,key1Bins);
        recordUpdates.put(TEST_KEY_2,key2Bins);

        // Lock one of the objects so the update will fail
        String differentTxnID = "1234";
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,differentTxnID);

        // Now try the update - it should throw an error
        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates);
            Assert.fail("Should throw a Lock Exception - did not - test failed");
        }
        catch(AerospikeClientWithTxnSupport.LockAcquireException e){

        }
        catch(Exception e){
            Assert.fail("Exception "+ e.getMessage()+ " thrown - should be "+ (new AerospikeClientWithTxnSupport.LockAcquireException(TEST_KEY_1,differentTxnID)).getClass().getName());
        }
        // Tidy up
        finally{
            aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
            aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);
            aerospikeClientWithTxnSupport.removeLock(TEST_KEY_1,differentTxnID);
        }
    }

    /**
     * Check that we get the correct result, with no locks, no txn record if no errors
     */
    @Test
    public void checkNoLocksNoTxnRecordCorrectResultForNoErrorTxn(){
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1,key1Bins);
        recordUpdates.put(TEST_KEY_2,key2Bins);

        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates);
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertTrue(getTransactions(aerospikeClientWithTxnSupport).size() == 0);

            Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
            Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

            Assert.assertTrue(record1.getInt("Bin-01") == 5);
            Assert.assertTrue(record1.getInt("Bin-02") == 6);
            Assert.assertTrue(record2.getInt("Bin-01") == 7);
            Assert.assertTrue(record2.getInt("Bin-02") == 8);

        }
        catch(Exception e){
            Assert.fail("Exception thrown - shouldn't happen\n"+e.getMessage());
        }
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_2);

    }

    /**
     * Check that we get the correct result, with no locks, no txn record if no errors
     */
    @Test
    public void checkNoLocksNoTxnRecordCorrectResultForNoErrorTxnWithGenCheck(){
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1,key1Bins);
        recordUpdates.put(TEST_KEY_2,key2Bins);

        HashMap<Key,Integer> generationCheckMap = new HashMap<Key,Integer>();
        generationCheckMap.put(TEST_KEY_1,1);
        generationCheckMap.put(TEST_KEY_2,1);

        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates,generationCheckMap);
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertTrue(getTransactions(aerospikeClientWithTxnSupport).size() == 0);

            Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
            Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

            Assert.assertTrue(record1.getInt("Bin-01") == 5);
            Assert.assertTrue(record1.getInt("Bin-02") == 6);
            Assert.assertTrue(record2.getInt("Bin-01") == 7);
            Assert.assertTrue(record2.getInt("Bin-02") == 8);

        }
        catch(Exception e){
            Assert.fail("Exception thrown - shouldn't happen\n"+e.getMessage());
        }
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_2);

    }

    /**
     * Check that we get the correct result, if the generation check fails
     * An error should be thrown
     * The locks should be released
     * The transaction record shoudl be removed
     * The original records should be as previously
     *
     */
    @Test
    public void checkCorrectResultWithGenFailCheck(){
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1,key1Bins);
        recordUpdates.put(TEST_KEY_2,key2Bins);

        HashMap<Key,Integer> generationCheckMap = new HashMap<Key,Integer>();
        generationCheckMap.put(TEST_KEY_1,1);
        generationCheckMap.put(TEST_KEY_2,2);

        String txnID = TxnSupport.uniqueTxnID();
        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates,generationCheckMap,txnID);
            Assert.fail("Should throw a Generation Fail Exception - did not - test failed");

            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertTrue(getTransactions(aerospikeClientWithTxnSupport).size() == 0);

            Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
            Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

            Assert.assertTrue(record1.getInt("Bin-01") == 5);
            Assert.assertTrue(record1.getInt("Bin-02") == 6);
            Assert.assertTrue(record2.getInt("Bin-01") == 7);
            Assert.assertTrue(record2.getInt("Bin-02") == 8);

        }
        catch(TxnSupport.GenFailException e){
            Assert.assertTrue(e.getKey().userKey.toString().equals(TEST_KEY_2.userKey.toString()));
        }
        catch(Exception e){
            Assert.fail("Exception "+e.getMessage()+" thrown - shouldn't happen");
        }
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));

        Assert.assertNull(aerospikeClientWithTxnSupport.get(testReadPolicy,aerospikeClientWithTxnSupport.keyForTxnID(txnID)));

        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

        Assert.assertTrue(record1.getInt("Bin-01") == 1);
        Assert.assertTrue(record1.getInt("Bin-02") == 2);
        Assert.assertTrue(record2.getInt("Bin-01") == 3);
        Assert.assertTrue(record2.getInt("Bin-02") == 4);

        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_2);

    }

    /**
     * Check that we get the correct result for a delete, with no locks, no txn record if no errors
     */
    @Test
    public void checkCorrectResultForDelete(){
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1,key1Bins);
        recordUpdates.put(TEST_KEY_2,null);

        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates);
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
            Assert.assertTrue(getTransactions(aerospikeClientWithTxnSupport).size() == 0);

            Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
            Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

            Assert.assertTrue(record1.getInt("Bin-01") == 5);
            Assert.assertTrue(record1.getInt("Bin-02") == 6);
            Assert.assertTrue(record2 == null);
        }
        catch(Exception e){
            Assert.fail("Exception thrown - shouldn't happen\n"+e.getMessage());
        }
        aerospikeClientWithTxnSupport.delete(new WritePolicy(),TEST_KEY_1);
    }

    /**
     *
     * Above test is
     * Check that if we look to update objects with keys key1/key2 then an exception is thrown
     * if one of those objects is already locked
     *
     * Make sure that we remove any locks added as part of that transaction
     */
    @Test
    public void locksRemovedWhenTxnFails() throws AerospikeClientWithTxnSupport.LockAcquireException{
        exceptionRaisedWhenRecordLocksExist();
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));
    }

    /**
     * Should be able to use a transaction record to rollback to previous state
     */
    @Test
    public void testRollback() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup
            =====
        */
        Key[] keyArray = new Key[]{TEST_KEY_1,TEST_KEY_2};
        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        // Create the transaction record
        // We can use this to rollback vs the updates below
        String txnID = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(keyArray, txnID);

        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords,txnID);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID);

        /*
            New object versions
            ===================
        */
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        Assert.assertTrue(aerospikeClientWithTxnSupport.txnIncomplete(txnID));
        try {
            aerospikeClientWithTxnSupport.rollback(txnID);
        }
        catch(AerospikeClientWithTxnSupport.KeyFormatException e){}

        // Check that we have reverted to previous state
        // No locks, no txn record, previous object state restored

        // No locks
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));

        // No transaction record
        Assert.assertFalse(aerospikeClientWithTxnSupport.txnIncomplete(txnID));

        // Previous record state restored
        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

        Assert.assertTrue(record1.getInt("Bin-01") == 1);
        Assert.assertTrue(record1.getInt("Bin-02") == 2);
        Assert.assertTrue(record2.getInt("Bin-01") == 3);
        Assert.assertTrue(record2.getInt("Bin-02") == 4);

        // Clean up
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);
    }

    /**
     * Should not be able to roll back a record that is already being rolled back
     */
    @Test
    public void testRollbackFailsIfRollbackInProgress() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup
            =====
        */
        Key[] keyArray = new Key[]{TEST_KEY_1,TEST_KEY_2};
        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        // Create the transaction record
        // We can use this to rollback vs the updates below
        String txnID = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(keyArray, txnID);

        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords,txnID);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID);

        /*
            New object versions
            ===================
        */
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        Assert.assertTrue(aerospikeClientWithTxnSupport.txnIncomplete(txnID));
        String inProgressRollbackTxnID = TxnSupport.uniqueTxnID();
        aerospikeClientWithTxnSupport.createLock(aerospikeClientWithTxnSupport.keyForTxnID(txnID),inProgressRollbackTxnID);
        try {
            aerospikeClientWithTxnSupport.rollback(txnID);
            Assert.fail("Rollback should have failed as transaction record is locked");
        }
        catch (TxnSupport.LockAcquireException e){
            // This error should be thrown
        }
        catch(AerospikeClientWithTxnSupport.KeyFormatException e){}

        // Check that we have reverted to previous state
        // No locks, no txn record, previous object state restored

        // Locks should still be in place
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));

        // Txn record should still exist
        Assert.assertTrue(aerospikeClientWithTxnSupport.txnIncomplete(txnID));

        // Current record state restored
        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

        Assert.assertTrue(record1.getInt("Bin-01") == 5);
        Assert.assertTrue(record1.getInt("Bin-02") == 6);
        Assert.assertTrue(record2.getInt("Bin-01") == 7);
        Assert.assertTrue(record2.getInt("Bin-02") == 8);

        // Clean up
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);

        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_2,txnID);
        aerospikeClientWithTxnSupport.removeLock(aerospikeClientWithTxnSupport.keyForTxnID(txnID),inProgressRollbackTxnID);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,aerospikeClientWithTxnSupport.keyForTxnID(txnID));

    }

    /**
     * Should be able to use a transaction record to rollback to previous state, including inserts
     */
    @Test
    public void testRollbackFromInserts() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup
            =====
        */
        Key[] keyArray = new Key[]{TEST_KEY_1,TEST_KEY_2};
        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        // Note no corresponding insert for TEST_KEY_1

        // Create the transaction record
        // We can use this to rollback vs the updates below
        String txnID = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(keyArray, txnID);

        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords,txnID);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID);

        Assert.assertTrue(aerospikeClientWithTxnSupport.txnIncomplete(txnID));

        /*
            New object versions
            ===================
        */
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        try {
            aerospikeClientWithTxnSupport.rollback(txnID);
        }
        catch(AerospikeClientWithTxnSupport.KeyFormatException e){}

        // Check that we have reverted to previous state
        // No locks, no txn record, previous object state restored

        // No locks
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));

        // No transaction record
        Assert.assertFalse(aerospikeClientWithTxnSupport.txnIncomplete(txnID));

        // Previous record state restored
        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

        Assert.assertTrue(record1.getInt("Bin-01") == 1);
        Assert.assertTrue(record1.getInt("Bin-02") == 2);
        Assert.assertTrue(record2 == null);

        // Clean up
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);
    }

    /**
     * Should be able to use a transaction record to rollback to previous state, including deletes
     */
    @Test
    public void testRollbackFromDeletes() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup
            =====
        */
        Key[] keyArray = new Key[]{TEST_KEY_1,TEST_KEY_2};
        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        // Create the transaction record
        // We can use this to rollback vs the updates below
        String txnID = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(keyArray, txnID);

        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords,txnID);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID);

        /*
            New object versions
            ===================
        */
        key1Bins[0] = new Bin("Bin-01",5);
        key1Bins[1] = new Bin("Bin-02",6);

        key2Bins[0] = new Bin("Bin-01",7);
        key2Bins[1] = new Bin("Bin-02",8);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);

        try {
            aerospikeClientWithTxnSupport.rollback(txnID);
        }
        catch(AerospikeClientWithTxnSupport.KeyFormatException e){}

        // Check that we have reverted to previous state
        // No locks, no txn record, previous object state restored

        // No locks
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));

        // No transaction record
        Assert.assertTrue(getTransactions(aerospikeClientWithTxnSupport).size() == 0);

        // Previous record state restored
        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);

        Assert.assertTrue(record1.getInt("Bin-01") == 1);
        Assert.assertTrue(record1.getInt("Bin-02") == 2);
        Assert.assertTrue(record2.getInt("Bin-01") == 3);
        Assert.assertTrue(record2.getInt("Bin-02") == 4);

        // Clean up
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);
    }

    /**
     * Expired transactions should be rolled back
     * Unexpired transactions should not be
     */
    @Test
    public void testExpiredTransactionRollback() throws AerospikeClientWithTxnSupport.LockAcquireException
    {
        /*
            Setup
            =====
        */
        // Create six objects associated with keys TEST_KEY_1 ..6
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01","OLD-01");
        key1Bins[1] = new Bin("Bin-02","OLD-02");

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01","OLD-03");
        key2Bins[1] = new Bin("Bin-02","OLD-04");

        Bin[] key3Bins = new Bin[2];
        key3Bins[0] = new Bin("Bin-01","OLD-05");
        key3Bins[1] = new Bin("Bin-02","OLD-06");

        Bin[] key4Bins = new Bin[2];
        key4Bins[0] = new Bin("Bin-01","OLD-07");
        key4Bins[1] = new Bin("Bin-02","OLD-08");

        Bin[] key5Bins = new Bin[2];
        key5Bins[0] = new Bin("Bin-01","OLD-09");
        key5Bins[1] = new Bin("Bin-02","OLD-10");

        Bin[] key6Bins = new Bin[2];
        key6Bins[0] = new Bin("Bin-01","OLD-11");
        key6Bins[1] = new Bin("Bin-02","OLD-12");

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_3,key3Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_4,key4Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_5,key5Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_6,key6Bins);

        // Create three separate transactions records and the associated locks
        String txnID1 = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords1 = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(new Key[]{TEST_KEY_1,TEST_KEY_2},txnID1);
        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords1,txnID1);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID1);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID1);

        String txnID2 = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords2 = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(new Key[]{TEST_KEY_3,TEST_KEY_4}, txnID2);
        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords2,txnID2);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_3,txnID2);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_4,txnID2);

        String txnID3 = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords3 = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(new Key[]{TEST_KEY_5,TEST_KEY_6},txnID3);
        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords3,txnID3);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_5,txnID3);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_6,txnID3);

        // Move the timestamp of two of the transactions back so they meet the expiry criteria
        aerospikeClientWithTxnSupport.put(testWritePolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID1),
                new Bin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME,System.currentTimeMillis() - TransactionManager.DEFAULT_TXN_EXPIRY_PERIOD_MILLIS));
        aerospikeClientWithTxnSupport.put(testWritePolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID2),
                new Bin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME,System.currentTimeMillis() - TransactionManager.DEFAULT_TXN_EXPIRY_PERIOD_MILLIS));

        // Now update all six objects
        key1Bins[0] = new Bin("Bin-01","NEW-01");
        key1Bins[1] = new Bin("Bin-02","NEW-02");

        key2Bins[0] = new Bin("Bin-01","NEW-03");
        key2Bins[1] = new Bin("Bin-02","NEW-04");

        key3Bins[0] = new Bin("Bin-01","NEW-05");
        key3Bins[1] = new Bin("Bin-02","NEW-06");

        key4Bins[0] = new Bin("Bin-01","NEW-07");
        key4Bins[1] = new Bin("Bin-02","NEW-08");

        key5Bins[0] = new Bin("Bin-01","NEW-09");
        key5Bins[1] = new Bin("Bin-02","NEW-10");

        key6Bins[0] = new Bin("Bin-01","NEW-11");
        key6Bins[1] = new Bin("Bin-02","NEW-12");

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_3,key3Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_4,key4Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_5,key5Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_6,key6Bins);

        // Rollback
        TransactionManager t = new TransactionManager(aerospikeClientWithTxnSupport);
        t.rollbackExpiredTxns();

        // Result should be that objects 1 - 4 are rolled back, objects 5 - 6 are not
        // There should be locks on objects 5 & 6 only
        // There should be a txn record for txn 3 only

        // Previous record state restored
        Record record1 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_1);
        Record record2 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_2);
        Record record3 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_3);
        Record record4 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_4);
        Record record5 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_5);
        Record record6 = aerospikeClientWithTxnSupport.get(testReadPolicy,TEST_KEY_6);

        Assert.assertTrue(record1.getString("Bin-01").equals("OLD-01"));
        Assert.assertTrue(record1.getString("Bin-02").equals("OLD-02"));
        Assert.assertTrue(record2.getString("Bin-01").equals("OLD-03"));
        Assert.assertTrue(record2.getString("Bin-02").equals("OLD-04"));
        Assert.assertTrue(record3.getString("Bin-01").equals("OLD-05"));
        Assert.assertTrue(record3.getString("Bin-02").equals("OLD-06"));
        Assert.assertTrue(record4.getString("Bin-01").equals("OLD-07"));
        Assert.assertTrue(record4.getString("Bin-02").equals("OLD-08"));
        Assert.assertTrue(record5.getString("Bin-01").equals("NEW-09"));
        Assert.assertTrue(record5.getString("Bin-02").equals("NEW-10"));
        Assert.assertTrue(record6.getString("Bin-01").equals("NEW-11"));
        Assert.assertTrue(record6.getString("Bin-02").equals("NEW-12"));

        Assert.assertTrue(aerospikeClientWithTxnSupport.get(testReadPolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID1)) == null);
        Assert.assertTrue(aerospikeClientWithTxnSupport.get(testReadPolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID2)) == null);
        Assert.assertTrue(aerospikeClientWithTxnSupport.get(testReadPolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID3)) != null);

        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_3));
        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_4));
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_5));
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_6));

        // Clean up
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_3);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_4);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_5);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_6);

        aerospikeClientWithTxnSupport.delete(testWritePolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID3));
        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_5,txnID3);
        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_6,txnID3);

    }

    private static Vector<Record> getTransactions(AerospikeClient client){
        ScanPolicy policy = new ScanPolicy();
        policy.concurrentNodes = true;
        policy.priority = Priority.LOW;
        policy.includeBinData = false;

        RecordRetrieverFromScan recordRetriever = new RecordRetrieverFromScan();
        client.scanAll(policy, TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,recordRetriever);
        return recordRetriever.getRecords();
    }

    private static class RecordRetrieverFromScan implements ScanCallback{
        Vector<Record> records = new Vector<Record>();

        public RecordRetrieverFromScan(){};

        public void scanCallback(Key key,Record record){
            records.addElement(record);
        }

        public Vector<Record> getRecords(){
            return records;
        }

    }

    /**
     * Orphan locks that have 'timed out' should be removed by removeOrphanLocks
     * Orphan locks that have not timed out should not be removed
     * Non-orphan locks should not be removed, regardless of timeout
     */
    @Test
    public void testOrphanLockHarvest() throws AerospikeClientWithTxnSupport.LockAcquireException{
        /*
            Setup transaction with locks
        */
        Key[] keyArray = new Key[]{TEST_KEY_1,TEST_KEY_2};
        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01",1);
        key1Bins[1] = new Bin("Bin-02",2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01",3);
        key2Bins[1] = new Bin("Bin-02",4);

        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_1,key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy,TEST_KEY_2,key2Bins);

        // Create the transaction record
        String txnID = TxnSupport.uniqueTxnID();
        HashMap<String,Map<String,Object>> txnRecords = aerospikeClientWithTxnSupport.existingVersionsOfRecordsForUpdate(keyArray, txnID);

        aerospikeClientWithTxnSupport.createTransactionRecord(TestConstants.TEST_NAMESPACE,txnRecords,txnID);

        aerospikeClientWithTxnSupport.createLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_2,txnID);

        TransactionManager t = new TransactionManager(aerospikeClientWithTxnSupport);

        String nonExistentTxnID = UUID.randomUUID().toString();
        aerospikeClientWithTxnSupport.createLock(TEST_KEY_3,nonExistentTxnID);

        // Test that orphan locks are not removed if timeout period not satisfied
        t.removeOrphanLocks();
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_3));

        // Check they are when timeout is satisfied
        aerospikeClientWithTxnSupport.put(testWritePolicy,AerospikeClientWithTxnSupport.lockKey(TEST_KEY_1),new Bin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME,System.currentTimeMillis() - TransactionManager.DEFAULT_TXN_EXPIRY_PERIOD_MILLIS));
        aerospikeClientWithTxnSupport.put(testWritePolicy,AerospikeClientWithTxnSupport.lockKey(TEST_KEY_2),new Bin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME,System.currentTimeMillis() - TransactionManager.DEFAULT_TXN_EXPIRY_PERIOD_MILLIS));
        aerospikeClientWithTxnSupport.put(testWritePolicy,AerospikeClientWithTxnSupport.lockKey(TEST_KEY_3),new Bin(AerospikeClientWithTxnSupport.TIMESTAMP_BIN_NAME,System.currentTimeMillis() - TransactionManager.DEFAULT_TXN_EXPIRY_PERIOD_MILLIS));
        t.removeOrphanLocks();

        Assert.assertFalse(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_3));

        // Check other locks are not removed
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_1));
        Assert.assertTrue(aerospikeClientWithTxnSupport.lockExists(TEST_KEY_2));

        // Tidy up
        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_1,txnID);
        aerospikeClientWithTxnSupport.removeLock(TEST_KEY_2,txnID);

        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_1);
        aerospikeClientWithTxnSupport.delete(testWritePolicy,TEST_KEY_2);

        aerospikeClientWithTxnSupport.delete(testWritePolicy,new Key(TestConstants.TEST_NAMESPACE, AerospikeClientWithTxnSupport.TRANSACTION_SET,txnID));
    }

    /**
     * Check byte array to string conversions are correct by doing byte[] -> string -> byte[]
     * and that original is recovered
     */
    @Test
    public void testByteArrayToStringConversion(){
        Key key = new Key(TestConstants.TEST_NAMESPACE,TestConstants.AEROSPIKE_TEST_SET_NAME,"ABCDE");
        String byteArrayAsString = AerospikeClientWithTxnSupport.byteArrayToString(key.digest);
        byte[] stringAsByteArray = AerospikeClientWithTxnSupport.stringToByteArray(byteArrayAsString);

        Assert.assertTrue(stringAsByteArray.length == key.digest.length);
        for(int i=0;i<key.digest.length;i++){
            Assert.assertTrue(key.digest[i] == stringAsByteArray[i]);
        }

    }

    /**
     * Check an error is thrown if using AerospikeClientWithTxnSupport in enterprise mode
     * but using vs Community. This also shows that durable deletes are enabled
     *
     * This test will fail if running vs Enterprise
     */
    @Test
    public void checkErrorThrownWhenIsEnterpriseTrueAndUsingCommunity() throws TxnSupport.LockAcquireException {
        /*
            Setup
            =====
        */

        // Create two objects associated with keys TEST_KEY_1 and TEST_KEY_2
        Bin[] key1Bins = new Bin[2];
        key1Bins[0] = new Bin("Bin-01", 1);
        key1Bins[1] = new Bin("Bin-02", 2);

        Bin[] key2Bins = new Bin[2];
        key2Bins[0] = new Bin("Bin-01", 3);
        key2Bins[1] = new Bin("Bin-02", 4);

        aerospikeClientWithTxnSupport.put(testWritePolicy, TEST_KEY_1, key1Bins);
        aerospikeClientWithTxnSupport.put(testWritePolicy, TEST_KEY_2, key2Bins);

        /*
            New object versions
            ===================
        */

        // Create new versions of objects with keys TEST_KEY_2 & TEST_KEY_2
        // We wish to update these objects as an atomic transaction
        key1Bins[0] = new Bin("Bin-01", 5);
        key1Bins[1] = new Bin("Bin-02", 6);

        key2Bins[0] = new Bin("Bin-01", 7);
        key2Bins[1] = new Bin("Bin-02", 8);

        HashMap<Key, Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
        recordUpdates.put(TEST_KEY_1, key1Bins);
        recordUpdates.put(TEST_KEY_2, key2Bins);

        aerospikeClientWithTxnSupport.setEnterprise(true);
        try {
            aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates);
            Assert.fail("An error should have been thrown if enabling enterprise, but using Community Edition");
        }
        catch (TxnSupport.TxnException e){
            Assert.assertTrue(e.getResultCode() == ResultCode.ENTERPRISE_ONLY);
        }
        catch(Exception e){
            Assert.fail("An error was thrown that was not a TxnException - investigate");
        }
    }
}
