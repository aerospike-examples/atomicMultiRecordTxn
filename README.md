# Aerospike - Atomic Multi-Record Txn Support

## This content is obsolete
[Aerospike Database 8](https://download.aerospike.com/artifacts/aerospike-server-enterprise/latest/) (and Java client [version 9.0.1](https://download.aerospike.com/download/client/java/notes.html)) adds multi-record transaction (MRT) functionality to `strong-consistency true` (SC) namespaces.

## Overview
This repository allows execution of atomic multi-record transactions within Aerospike.

It does this by

1.	Exclusively 'locking' objects to be updated (txn fails if lock cannot be acquired)
2.	Recording the existing state of records in a 'transaction' record
3.	Performing the updates
4.	Deleting the txn record indicating that the txn is complete
5.	Releasing locks

A TransactionManager class is available for rolling back failed transactions and for removing timed out orphan locks.

## Caveats

Although transactions are atomic, they are not isolated. Dirty reads are possible, although you can mitigate this by checking whether records are currently part of a transaction (locked), or by making use of optimistic locking techniques (see below).

Use of this library does not guard against non-transactional single record (standard) use, although the [generation check capability](#genCheck) goes some way to guarding against this. Recommended strategy is to mandate use of this library for all operations on objects which might be part of multi-record transactions. This deals with the above caveat.

The locking method used in this repository has been criticized - see [http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html](http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). In summary this says that seemingly aborted txns and orphan locks could still be active in pathological scenarios. Although this is unlikely, the implications for your own use case should be carefully considered.

It is designed for relatively low volume use. You should test to see if it is appropriate for your needs. There may be potential possible optimizations.

## Usage

### Basic Atomic Write

```java
// transactionNamespace is used to hold locks and transaction records
AerospikeClientWithTxnSupport aerospikeClientWithTxnSupport =
new AerospikeClientWithTxnSupport(clientPolicy, serverIP, serverPort, transactionNamespace);

// Keys can be in any namespace or set
Key KEY_1 = new Key(namespace,set,userKey1);
Key KEY_2 = new Key(namespace2,set2,userKey2);
HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
recordUpdates.put(KEY_1,key1Bins);
recordUpdates.put(KEY_2,key2Bins);

// Throws LockAcquireException
aerospikeClientWithTxnSupport.put(testWritePolicy, recordUpdates);
```

The put is a multi-record put.

### Rollback

Rollback of incomplete txns is via the TransactionManager class

```java
// Set up client with transaction support
AerospikeClientWithTxnSupport client = 
new AerospikeClientWithTxnSupport(clientPolicy, serverIP, serverPort, transactionNamespace);

// and transaction manager
TransactionManager tm = new TransactionManager(client);
tm.setTransactionTimeOutMillis(yourTimeoutValue);

tm.rollbackExpiredTxns();
tm.removeOrphanLocks();
```

This should be run with a given frequency, or preceding each update

### <a name="genCheck"></a>Atomic write incorporating generation check

Using generation check to make sure records have not been updated via Single Record transactions

```java
AerospikeClientWithTxnSupport aerospikeClientWithTxnSupport =
        new AerospikeClientWithTxnSupport(clientPolicy, serverIP, serverPort, transactionNamespace);

Key KEY_1 = new Key(namespace,set,userKey1);
Key KEY_2 = new Key(namespace2,set2,userKey2);

int key1CurrentGen = aerospikeClientWithTxnSupport.get(readPolicy,KEY_1).generation;
int key2CurrentGen = aerospikeClientWithTxnSupport.get(readPolicy,KEY_2).generation;
        
HashMap<Key,Bin[]> recordUpdates = new HashMap<Key, Bin[]>();
HashMap<Key,Integer> generationCheckMap = new HashMap<Key, Integer>();

recordUpdates.put(KEY_1,key1Bins);
recordUpdates.put(KEY_2,key2Bins);

generationCheckMap.put(KEY_1,key1CurrentGen);
generationCheckMap.put(KEY_2,key2CurrentGen);

// Throws LockAcquireException and GenCheckException
aerospikeClientWithTxnSupport.put(writePolicy,recordUpdates,generationCheckMap);
```

If you have further questions you may find the answers in the [FAQ](FAQ.md). 

JavaDoc available at [javadoc](javadoc/index.html)

Jar available at [multiRecordTxn.jar](artifacts/multi-record-txn.jar)

## Performance

No attempt has been made to verify whether this is performant. It has been created to fulfil a low frequency requirement

## Enterprise / Community

Note that Community Aerospike does not support [durable deletes](https://aerospike.com/docs/guide/durable_deletes.html). In the event of a cold start, lock and transaction records can be resurrected, making Community unsafe in this context. For that reason, durable deletes are used by default. You will get an error if trying to use this API with Community Edition therefore. However, you can call ```isEnterprise(false)``` to disable use of durable deletes.

The value of this API is greater also if using the Aerospike Enterprise [Strong Consistency](https://www.aerospike.com/docs/architecture/consistency.html) option. Strong consistency gives you a guarantee that duplicate records in your database ( necessary for resilience purposes ) will not ever experience divergence. Without this ( which very few databases in our performance range offer ) there is potential for divergence to occur in the event of network partitions and process crashes. Divergence of records here would mean locks or transaction records being lost in a sub-cluster experiencing a partition event ( or process crash ). 

## Feedback

Please use the [issues](../../issues) feature.

## Testing

Comprehensive unit-testing has been undertaken - see [AerospikeClientWithTxnSupportTest.java](src/test/java/com/aerospike/txnSupport/AerospikeClientWithTxnSupportTest.java)

Also a multi-threaded banking simulation - [AccountTransferSimulationTest.java](src/test/java/com/aerospike/txnSupport/AccountTransferSimulationTest.java). This mimics the classic use case of a necessarily atomic debit and credit.

* 300 concurrent threads 
* Each executes 100 consecutive transactions
* A transaction is a transfer of a randomly generated monetary amount between two randomly chosen accounts from a population of 1000
* The parameters above can be modified
* Transfers will fail if exclusive locks cannot be acquired.
* The test checks at the end that money has not been created or destroyed. This would happen if locking was not used due to race conditions.

### Sample Output

```
Running banking simulation

100 iterations of 300 concurrent transactions across 1000 accounts
-------------------
Iteration 0001 of 100
Iteration 0002 of 100
Iteration 0003 of 100
....
Iteration 0097 of 100
Iteration 0098 of 100
Iteration 0099 of 100
Iteration 0100 of 100
-------------------
Simulation completed successfully - starting money  = ending money

Attempted txns : 30000
Successful txns : 10914
Failed txns : 19086

```

Also tested was the above simulation when a database failure occurs. The simulation is running when a rule

```bash
iptables -A INPUT -p tcp --destination-port 3000 -j DROP
```

is added which stops communication with the database. Once the simulation has timed out execute 

```bash
iptables -D INPUT -p tcp --destination-port 3000 -j DROP
```

to re-enable followed by execution of the main method in [ConsistentAfterFailureTest.java](src/test/java/com/aerospike/txnSupport/ConsistentAfterFailureTest.java)

This performs a rollback of uncommitted transations and removes orphan locks. It then checks again that money has not been created or destroyed. Statisitics concerning number of rolled back transactions are shown.

**Note** Stopping a one node cluster as a way of performing this test will not work as you will lose writes in the streaming write buffer.

### Sample Output

```
Found 42 incomplete transactions

Pre-rollback, money in simulation did not match expected money in simulation

Money in simulation : 1000003205
Expected money in simulation : 1000000000

Post rollback, simulation completed successfully - starting money  = ending money
```
