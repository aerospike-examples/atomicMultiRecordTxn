# Frequenty Asked Questions

Some asked, some imagined.

**This API will ensure atomicity if everyone uses the API. What happens if they don't? How can you guard against non-atomic writes?**

See the [generation check example](README.md#atomic-write-incorporating-generation-check). This can be done by checking the generation of the record. If the generation has changed, the transaction will fail with a Generation Check error.

**Don't you have to do this anyway i.e. even if you use the API for all updates?**

No, because the locking will prevent concurrent updates if the API is used exclusively on the records in question.

**All writes to all records under multi-record transactions implementation must follow this path. If, for example, single record updates are performed directly, can they be lost during abort?**

That is true. In this situation it is difficult to know what the right thing to do would be. Rollback throws an error - yes that probably should happen. Either way you lose though - if you don't rollback you preserve a potentially non-atomic update. If you do roll back, you lose a write. 

To *emphasize* however, if this is a concern, perform single record updates via the API as well. The intended usage is however that MRT will be reserved for a subset of records, and MRT should be used for this subset.

**Can reads be made under lock to avoid reading dirty data?**

You can do this. Simply make use of the ```createLock(Key key, String txnID)```

**Can I check if a record is locked before reading it?**

Use ```boolean lockExists(Key key)```

**Does the implementation handle uncertain writes (in-doubt or client/connection/server failures) appropriately?**

If an error is thrown, it will contain the transaction id. You can then make use of ```TxnSupport.isComplete(String txnID)```

**Can an application check for the status of a multi-record transaction after the fact?**

Overloaded versions of the put call will take txnID as a parameter, which can be generated via ```TxnSupport.uniqueTxnID()```. You can then use that to check status using the call above. The transaction record contains details of all records affected by the transaction.

**What error handling do you supply? If you for example call rollback and a timeout error is thrown, how can you know if any / all parts of the transaction have been rolled back or not. Similarly, if createLock() throws a Timeout exception, can you find out if the lock exists or not?**

Exceptions emanating from the server are captured as non-checkable [AerospikeExceptions](https://www.aerospike.com/apidocs/java/com/aerospike/client/AerospikeException.html). This API wraps those in a TxnException class which provides the txnID which allows errors to be handled at the client side, including use of the ```isComplete``` call. Similarly for GenCheckException. LockAcquireException will be thrown before the transaction commences. The object key triggering the error will be identified in the error object. The ```lockExists``` call can then be used.

**Will this work with keys of all types - Aerospike allows Integer keys for example? I got the feeling it might not**

Yes it will. Internally we make use of the key [digests](https://discuss.aerospike.com/t/faq-how-keys-and-digests-are-used-in-aerospike/4663) which reflect the types used for keys.

**Something I like about Aerospike is the high level of control you have in your read and write operations via use of [Policies](https://www.aerospike.com/docs/guide/policies.html) Does this API allow me to maintain that control?**

You can specifically set the policies used in creating, reading and deleting lock and transaction records via the ```setTxnReadPolicy(Policy txnReadPolicy)``` and ```setTxnWritePolicy(WritePolicy txnWritePolicy)``` calls. The put methods accept a ```Policy writePolicy``` argument, which is used in executing the writes for those records.

**What if two rollback processes operate simultaneously - is there a race condition here?**

Rollback puts a lock on the transaction record of the transaction being rolled back so no - simultaneous rollback is not possible.



