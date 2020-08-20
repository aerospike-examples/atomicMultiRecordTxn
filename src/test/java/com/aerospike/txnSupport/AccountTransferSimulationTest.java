package com.aerospike.txnSupport;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.HashMap;
import java.util.Random;

public class AccountTransferSimulationTest {
    /**
     * Member variables
     */
    // Simulation variables
    private int noOfAccounts = DEFAULT_NO_OF_ACCOUNTS;
    private int initialBalanceAmountPence  = DEFAULT_BALANCE_AMOUNT_PENCE;
    private int maxTransferAmount = DEFAULT_MAX_TRANSFER_AMOUNT;

    private int successfulTransactions = 0;
    private int failedTransactions = 0;
    private int totalAttemptedTransactions = 0;

    // Account key attributes
    private int accountDigits = DEFAULT_ACCOUNT_DIGITS;
    private String accountPrefix = DEFAULT_ACCOUNT_PREFIX;

    // Storage
    private String simulationNamespace;
    private String simulationSetName = DEFAULT_SIMULATION_SET_NAME;

    // Database related
    private AerospikeClientWithTxnSupport client;
    private final Policy SIMULATION_READ_POLICY = new Policy();
    private final WritePolicy SIMULATION_WRITE_POLICY = new WritePolicy();

    // Defaults
    public static final int DEFAULT_NO_OF_ACCOUNTS = 1000;
    public static final String DEFAULT_SIMULATION_SET_NAME = "AccountTransferSimulationTest";
    public static final int DEFAULT_ACCOUNT_DIGITS = 6;
    public static final String DEFAULT_ACCOUNT_PREFIX = "ACCOUNT-";
    public static final int DEFAULT_BALANCE_AMOUNT_PENCE=1000000; // 10,000 £ or $
    public static final int DEFAULT_MAX_TRANSFER_AMOUNT = 10000; // £/$100

    // Bin Names for account records
    public static final String ACCOUNT_ID_BIN_NAME = "account-id";
    public static final String BALANCE_PENCE_BIN_NAME = "balance-pence";

    // Saving simulation state
    public static final String SIMULATION_RECORD_TYPE = "accountTransferSimulation";
    public static final String SIMULATION_DETAIL_USER_KEY = "316e8859-7f2b-4606-8415-93c6ef7040b3";
    public static final String NO_OF_ACCOUNTS_BIN_NAME = "noOfAccounts";
    public static final String INITIAL_BALANCE_PENCE_BIN_NAME = "initBalance";
    public static final String SIMULATION_DETAIL_SET = "account-transfer-simulation";

    /**
     * Create object that will manage the account transfer simulation
     *
     * @param client - AerospikeClient object to manage persistence of simulation
     * @param namespace - namespace used for simulation
     * @param noOfAccounts - number of accounts created by simulation
     * @param initialBalanceAmountPence - initial balance of each account
     */
    public AccountTransferSimulationTest(AerospikeClientWithTxnSupport client, String namespace, int noOfAccounts, int initialBalanceAmountPence){
        this.simulationNamespace = namespace;
        this.client = client;
        this.noOfAccounts = noOfAccounts;
        this.initialBalanceAmountPence = initialBalanceAmountPence;
    }

    /**
     * No of accounts set up by simulation
     * @return number of accounts in simulation
     */
    public int getNoOfAccounts() {
        return noOfAccounts;
    }

    /**
     * Max transfer amount used in simulation, in pence
     * @return int
     */
    public int getMaxTransferAmount() {
        return maxTransferAmount;
    }

    /**
     * Initialize an account balance
     *
     * @param accountID - account ID
     * @param balanceInPence - balance in pence
     * @throws AccountExistsException if account already exists
     */
    public void createAccountBalance(String accountID,int balanceInPence) throws AccountExistsException{
        Key key = new Key(simulationNamespace, simulationSetName,accountID);
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        try{
            client.put(policy,key,new Bin(ACCOUNT_ID_BIN_NAME,accountID),new Bin(BALANCE_PENCE_BIN_NAME,balanceInPence));
        }
        catch(AerospikeException ae){
            if(ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                throw new AccountExistsException(accountID);
            }
            else{
                throw ae;
            }
        }
    }

    /**
     * Initalize all account balances in simulation, using defaults for number of accounts and initial balance
     * @throws AccountExistsException if account already exists
     */
    public void batchInitializeAccountBalances() throws AccountExistsException{
        for(int i=1;i<=noOfAccounts;i++){
            createAccountBalance(accountIDFromInt(i),initialBalanceAmountPence);
        }
    }

    /**
     * Remove all account records
     */
    public void removeAccounts(){
        for(int i=1;i<=noOfAccounts;i++){
            client.delete(SIMULATION_WRITE_POLICY,new Key(simulationNamespace,simulationSetName,accountIDFromInt(i)));
        }
    }

    /**
     * Database key for account record, given account id as string
     *
     * @param accountID - account id
     * @return Database key
     */
    public Key accountKey(String accountID){
        return new Key(simulationNamespace,simulationSetName,accountID);
    }

    /**
     * Database key for account record given account id as integer
     * @param accountID - account id as int
     * @return Database key
     */
    public Key accountKey(int accountID){
        return accountKey(accountIDFromInt(accountID));
    }

    /**
     * Account id as string from an integer value
     * @param accountNo - integer identifying an account
     * @return account id as a string
     */
    public String accountIDFromInt(int accountNo){
        return accountPrefix + String.format("%0"+accountDigits+"d",accountNo);
    }

    /**
     * Transfer amountPence atomically from fromAccount to toAccount
     * @param fromAccount - account from which money will be transferred
     * @param toAccount - account to which money will be transferred
     * @param amountPence - transfer amount
     * @throws AerospikeClientWithTxnSupport.LockAcquireException if either of the accounts is currently locked
     */
    public void transfer(String fromAccount,String toAccount,int amountPence) throws AerospikeClientWithTxnSupport.LockAcquireException {
        String txnID = TxnSupport.uniqueTxnID();
        Key fromAccountKey = accountKey(fromAccount);
        Key toAccountKey = accountKey(toAccount);

        client.createLock(fromAccountKey,txnID);
        try {
            client.createLock(toAccountKey, txnID);
        }
        // Remove lock just taken if can't acquire second
        catch(AerospikeClientWithTxnSupport.LockAcquireException e){
            client.removeLock(fromAccountKey,txnID);
            throw(e);
        }

        Record fromAccountRecord = client.get(SIMULATION_READ_POLICY,fromAccountKey);
        Record toAccountRecord = client.get(SIMULATION_READ_POLICY,toAccountKey);

        long fromAccountBalance = fromAccountRecord.getInt(BALANCE_PENCE_BIN_NAME);
        long toAccountBalance = toAccountRecord.getInt(BALANCE_PENCE_BIN_NAME);

        fromAccountBalance = fromAccountBalance - amountPence;
        toAccountBalance = toAccountBalance + amountPence;

        Bin[] fromAccountBins = new Bin[]{new Bin(ACCOUNT_ID_BIN_NAME,fromAccount),new Bin(BALANCE_PENCE_BIN_NAME,fromAccountBalance)};
        Bin[] toAccountBins = new Bin[]{new Bin(ACCOUNT_ID_BIN_NAME,toAccount),new Bin(BALANCE_PENCE_BIN_NAME,toAccountBalance)};

        HashMap<Key,Bin[]> recordsForUpdate = new HashMap<Key,Bin[]>();
        recordsForUpdate.put(fromAccountKey,fromAccountBins);
        recordsForUpdate.put(toAccountKey,toAccountBins);

        client.put(SIMULATION_WRITE_POLICY,recordsForUpdate,txnID);
    }

    /**
     * Get total money in simulation ( sum all balances )
     * @return total amount of money summed across all accounts in simulation
     */
    public long getTotalMoneyInSimulation(){
        long total = 0;
        for(int i=1;i<=noOfAccounts;i++){
            total += client.get(SIMULATION_READ_POLICY,accountKey(i)).getLong(BALANCE_PENCE_BIN_NAME);
        }
        return total;
    }

    /**
     * Get expected money in simulation ( should be what we started with )
     *
     * @return expected amount of money in simulation as long
     */
    public long getExpectedMoneyInSimulation(){
        return noOfAccounts * initialBalanceAmountPence;
    }

    /**
     * We simulate a banking transaction environment
     * We create 1000 accounts and 100 threads
     * Each thread picks two accounts at random and transfers money between then atomically
     * This is iterated over 100 times
     * If we cannot get the required locks, the transactions are cancelled
     * At the end of the simulation we check that the money we started with is equal to the current money
     * If this is not true, throw an exception
     *
     * @param args - command line arguments - see usage
     * @throws AccountExistsException - if accounts already exist. Generally this means an incomplete simulation has been run
     * @throws InterruptedException - we wait for threads - this can in theory happen
     * @throws TestFailedException - test failed for some reason
     */
    public static void main(String[] args) throws AccountExistsException, InterruptedException, TestFailedException
    {
        // Constants for Simulation
        int noOfAccounts = 1000;
        int concurrentActions = 300;
        int simulationIterations = 100;

        // Set up Database connection
        String simulationNamespace = TestConstants.TEST_NAMESPACE;
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = concurrentActions;
        AerospikeClientWithTxnSupport client =
                new AerospikeClientWithTxnSupport(clientPolicy, TestConstants.AEROSPIKE_SERVER_IP, TestConstants.AEROSPIKE_SERVER_PORT, TestConstants.TEST_TXN_NAMESPACE);
        client.setEnterprise(false);

        // Set up simulation
        AccountTransferSimulationTest simulation  = new AccountTransferSimulationTest(client,simulationNamespace, noOfAccounts,DEFAULT_BALANCE_AMOUNT_PENCE);
        simulation.batchInitializeAccountBalances();
        simulation.saveState();

        System.out.println("Running banking simulation\n");
        System.out.println(simulationIterations + " iterations of "+concurrentActions+" concurrent transactions across "+noOfAccounts+" accounts");
        System.out.println("-------------------");
        // Set up concurrent actions
        Thread[] threads = new Thread[concurrentActions];
        SingleTransaction[] singleTransactionRunnable = new SingleTransaction[concurrentActions];
        // which are iterated over proscribed number of times
        for (int i = 0; i < concurrentActions; i++) {
            singleTransactionRunnable[i] = new SingleTransaction(simulation, simulationIterations, i==simulationIterations -1);
            threads[i] = new Thread(singleTransactionRunnable[i]);
        }
        // Start threads
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        // Wait for completion
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
            simulation.successfulTransactions+= singleTransactionRunnable[i].successful;
            simulation.failedTransactions+=singleTransactionRunnable[i].failed;
            simulation.totalAttemptedTransactions+=singleTransactionRunnable[i].total;
        }

        System.out.println("-------------------");
        // check simulation was successful
        if(simulation.getTotalMoneyInSimulation() == simulation.getExpectedMoneyInSimulation()){
            System.out.println("Simulation completed successfully - starting money  = ending money");
            System.out.println("");
            System.out.println("Attempted txns : "+simulation.totalAttemptedTransactions);
            System.out.println("Successful txns : " + simulation.successfulTransactions);
            System.out.println("Failed txns : "+ simulation.failedTransactions);
            simulation.removeAccounts();
            simulation.removeState();
        }
        else
        {
            throw new TestFailedException("There is a problem - starting money " + simulation.getExpectedMoneyInSimulation() + " != ending money "+simulation.getTotalMoneyInSimulation());
        }
    }

    /**
     * Runnable object used to support banking simulation
     * Object executes one transaction
     */
    public static class SingleTransaction implements Runnable{
        private AccountTransferSimulationTest simulation;
        private Random random = new Random();
        private int simulationIterations;
        int successful = 0;
        int failed = 0;
        int total = 0;
        boolean isReporter;

         public SingleTransaction(AccountTransferSimulationTest simulation, int simulationIterations, boolean isReporter){
             this.simulation = simulation;
             this.simulationIterations = simulationIterations;
             this.isReporter = isReporter;
         }

        public void run(){
             for(int i=1;i<=simulationIterations;i++) {
                 // Randomly choose two accounts
                 int accountNo1 = random.nextInt(simulation.getNoOfAccounts()) + 1;
                 int accountNo2 = random.nextInt(simulation.getNoOfAccounts()) + 1;
                 // Make sure they're different
                 while (accountNo1 == accountNo2) {
                     accountNo2 = random.nextInt(simulation.getNoOfAccounts()) + 1;
                 }
                 int transferAmount = random.nextInt(simulation.getMaxTransferAmount()) + 1;
                 try {
                     simulation.transfer(simulation.accountIDFromInt(accountNo1), simulation.accountIDFromInt(accountNo2), transferAmount);
                     successful++;
                 } catch (AerospikeClientWithTxnSupport.LockAcquireException e) {
                     failed++;
                 }
                 total++;
                 if(isReporter){
                     System.out.println(String.format("Iteration %04d of "+simulationIterations,i));
                 }
             }
        }
    }

    /**
     * Persist simulation state - used by ConsistentAfterFailureTest test
     */
    public void saveState(){
        Bin[] bins = new Bin[3];
        bins[0] = new Bin(Constants.TYPE_BIN_NAME,SIMULATION_RECORD_TYPE);
        bins[1] = new Bin(NO_OF_ACCOUNTS_BIN_NAME,noOfAccounts);
        bins[2] = new Bin(INITIAL_BALANCE_PENCE_BIN_NAME,initialBalanceAmountPence);

        client.put(SIMULATION_WRITE_POLICY,stateKey(),bins);
    }

    /**
     * Remove simulation state record
     */
    public void removeState(){
        client.delete(SIMULATION_WRITE_POLICY,stateKey());
    }

    /**
     * Simulation state key
     * @return
     */
    private Key stateKey(){
        return stateKey(simulationNamespace);
    }

    static Key stateKey(String namespace){
        return new Key(namespace,SIMULATION_DETAIL_SET,SIMULATION_DETAIL_USER_KEY);
    }

    public static class AccountExistsException extends Exception {
        public AccountExistsException(String accountID) {super("Record for account " + accountID + " already exists");}
    }

    public static class TestFailedException extends Exception{
        public TestFailedException(String message){super(message);}
    }

}
