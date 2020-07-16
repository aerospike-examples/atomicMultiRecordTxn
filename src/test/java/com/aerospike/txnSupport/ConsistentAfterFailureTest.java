package com.aerospike.txnSupport;

import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;

/**
 * Purpose of class is to determine whether following an AccountTransferSimulationTest, the amount of money in total is as expected after rollback of uncommitted transactions
 *
 * Most useful if the simulation has been deliberately interrupted e.g. by using
 *
 * iptables -A INPUT -p tcp --destination-port 3000 -j DROP
 *
 * during the simulation to force unavailability
 */
public class ConsistentAfterFailureTest {
    public static void main(String[] args) throws AccountTransferSimulationTest.TestFailedException, TxnSupport.LockAcquireException {
        String simulationNamespace = TestConstants.TEST_NAMESPACE;

        // Set up client with transaction support
        AerospikeClientWithTxnSupport client =
                new AerospikeClientWithTxnSupport(new ClientPolicy(), TestConstants.AEROSPIKE_SERVER_IP, TestConstants.AEROSPIKE_SERVER_PORT,TestConstants.TEST_TXN_NAMESPACE);
        client.setEnterprise(false);

        // and transaction manager
        TransactionManager tm = new TransactionManager(client);
        tm.setTransactionTimeOutMillis(0);

        Record simulationState = client.get(new Policy(),AccountTransferSimulationTest.stateKey(simulationNamespace));
        if(simulationState == null){
            throw new AccountTransferSimulationTest.TestFailedException("No simulation state record found - cannot run this test");
        }

        // Initiate simulation object to make use of helper methods
        AccountTransferSimulationTest simulation = new AccountTransferSimulationTest(client,simulationNamespace,
                simulationState.getInt(AccountTransferSimulationTest.NO_OF_ACCOUNTS_BIN_NAME),simulationState.getInt(AccountTransferSimulationTest.INITIAL_BALANCE_PENCE_BIN_NAME));

        long preRollbackMoneyInSimulation  = simulation.getTotalMoneyInSimulation();

        // Roll back unfinished transactions using Transaction Manager
        int unfinishedTxns = tm.rollbackExpiredTxns();

        // Note number of unfinished transactions
        System.out.println("Found "+unfinishedTxns+" incomplete transactions");
        if(preRollbackMoneyInSimulation != simulation.getExpectedMoneyInSimulation()){
            System.out.println("");
            System.out.println("Pre-rollback, money in simulation did not match expected money in simulation");
            System.out.println("");
            System.out.println("Money in simulation : "+ preRollbackMoneyInSimulation);
            System.out.println("Expected money in simulation : " + simulation.getExpectedMoneyInSimulation());
            System.out.println("");
        }

        // Check system is consistent
        if(simulation.getTotalMoneyInSimulation() == simulation.getExpectedMoneyInSimulation()){
            System.out.println("Post rollback, simulation completed successfully - starting money  = ending money");
            // If so, clean up
            simulation.removeAccounts();
            simulation.removeState();
            tm.removeOrphanLocks();
        }
        // Throw an error if not
        else
        {
            throw new AccountTransferSimulationTest.TestFailedException("There is a problem - starting money " + simulation.getExpectedMoneyInSimulation() + " != ending money "+simulation.getTotalMoneyInSimulation());
        }
    }
}
