#include "global.h"
#include "message.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "timer.h"
#include "chain.h"

/**
 * Processes an incoming client batch and sends a Pre-prepare message to al replicas.
 *
 * This function assumes that a client sends a batch of transactions and 
 * for each transaction in the batch, a separate transaction manager is created. 
 * Next, this batch is forwarded to all the replicas as a BatchRequests Message, 
 * which corresponds to the Pre-Prepare stage in the PBFT protocol.
 *
 * @param msg Batch of Transactions of type CientQueryBatch from the client.
 * @return RC
 */
RC WorkerThread::process_client_batch(Message *msg)
{
    //printf("ClientQueryBatch: %ld, THD: %ld :: CL: %ld :: RQ: %ld\n",msg->txn_id, get_thd_id(), msg->return_node_id, clbtch->cqrySet[0]->requests[0]->key);
    //fflush(stdout);

    ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;

    // Authenticate the client signature.
    validate_msg(clbtch);

#if VIEW_CHANGES
    // If message forwarded to the non-primary.
    if (g_node_id != get_current_view(get_thd_id()))
    {
        client_query_check(clbtch);
        return RCOK;
    }

    // Partial failure of Primary 0.
    fail_primary(msg, 9);
#endif

    // Initialize all transaction mangers and Send BatchRequests message.
    create_and_send_batchreq(clbtch, clbtch->txn_id);

    return RCOK;
}

/**
 * Process incoming BatchRequests message from the Primary.
 *
 * This function is used by the non-primary or backup replicas to process an incoming
 * BatchRequests message sent by the primary replica. This processing would require 
 * sending messages of type PBFTPrepMessage, which correspond to the Prepare phase of 
 * the PBFT protocol. Due to network delays, it is possible that a repica may have 
 * received some messages of type PBFTPrepMessage and PBFTCommitMessage, prior to 
 * receiving this BatchRequests message.
 *
 * @param msg Batch of Transactions of type BatchRequests from the primary.
 * @return RC
 */
RC WorkerThread::process_batch(Message *msg)
{
    uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;
#if AHL
    if(breq->TwoPC_Vote_recvd || breq->TwoPC_Commit_recvd)
    {
        printf("BatchRequests: TID:%ld : RC_TID:%ld VIEW: %ld : THD: %ld\n",breq->txn_id, breq->rc_txn_id,breq->view, get_thd_id());
        if(breq->TwoPC_Request_recvd)cout<<"BatchRequest request 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        if(breq->TwoPC_Vote_recvd)cout<<"BatchRequest vote 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        if(breq->TwoPC_Commit_recvd)cout<<"BatchRequest commit 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        fflush(stdout);
    }
#endif
    // Assert that only a non-primary replica has received this message.
    assert(g_node_id != get_current_view(get_thd_id()));

    // Check if the message is valid.
    //validate_msg(breq);

#if VIEW_CHANGES
    // Store the batch as it could be needed during view changes.
    store_batch_msg(breq);
#endif

    // Allocate transaction managers for all the transactions in the batch.
    set_txn_man_fields(breq, 0);

#if TIMER_ON
    // The timer for this client batch stores the hash of last request.
    add_timer(breq, txn_man->get_hash());
#endif

    // Storing the BatchRequests message.
    txn_man->set_primarybatch(breq);

    // Send Prepare messages.
    txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
    double timepre = get_sys_clock() - cntime;
    INC_STATS(get_thd_id(), time_pre_prepare, timepre);

    // Only when BatchRequests message comes after some Prepare message.
    for (uint64_t i = 0; i < txn_man->info_prepare.size(); i++)
    {
        // Decrement.
        uint64_t num_prep = txn_man->decr_prep_rsp_cnt();
        if (num_prep == 0)
        {
            txn_man->set_prepared();
            break;
        }
    }

    // If enough Prepare messages have already arrived.
    if (txn_man->is_prepared())
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
        INC_STATS(get_thd_id(), time_prepare, timeprep);
        double timediff = get_sys_clock() - cntime;

        // Check if any Commit messages arrived before this BatchRequests message.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            uint64_t num_comm = txn_man->decr_commit_rsp_cnt();
            if (num_comm == 0)
            {
                txn_man->set_committed();
                break;
            }
        }

        // If enough Commit messages have already arrived.
        if (txn_man->is_committed())
        {
#if TIMER_ON
            // End the timer for this client batch.
            server_timer->endTimer(txn_man->hash);
#endif
            // Proceed to executing this batch of transactions.
            send_execute_msg();

            // End the commit counter.
            INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
        }
    }
    else
    {
        // Although batch has not prepared, still some commit messages could have arrived.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            txn_man->decr_commit_rsp_cnt();
        }
    }

#if AHL
    /*if(txn_man->is_2PC_Request_recvd())
        cout<<"Inside process_batch: 2PC request set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;*/
    if(txn_man->is_2PC_Vote_recvd())
        cout<<"Inside process_batch: 2PC vote set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;
    if(txn_man->is_2PC_Commit_recvd())
        cout<<"Inside process_batch: 2PC commit set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;
    fflush(stdout);
#endif

    // Release this txn_man for other threads to use.
    bool ready = txn_man->set_ready();
    assert(ready);

    // UnSetting the ready for the txn id representing this batch.
    txn_man = get_transaction_manager(msg->txn_id, 0);
    while (true)
    {
        bool ready = txn_man->unset_ready();
        if (!ready)
        {
            continue;
        }
        else
        {
            break;
        }
    }

    return RCOK;
}

#if AHL
/**
 * Process incoming BatchRequests message from the Primary for the second local pbft in 2PC.
 *
 * This function is used by the non-primary or backup replicas to process an incoming
 * BatchRequests message sent by the primary replica. This processing would require 
 * sending messages of type PBFTPrepMessage, which correspond to the Prepare phase of 
 * the PBFT protocol. Due to network delays, it is possible that a repica may have 
 * received some messages of type PBFTPrepMessage and PBFTCommitMessage, prior to 
 * receiving this BatchRequests message.
 *
 * @param msg Batch of Transactions of type BatchRequests from the primary.
 * @return RC
 */
RC WorkerThread::process_batch2(Message *msg)
{
    uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;

    if(breq->TwoPC_Vote_recvd || breq->TwoPC_Commit_recvd)
    {
        printf("BatchRequests2: TID:%ld : RC_TID:%ld VIEW: %ld : THD: %ld\n",breq->txn_id, breq->rc_txn_id,breq->view, get_thd_id());
        if(breq->TwoPC_Request_recvd)cout<<"BatchRequest2 request 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        if(breq->TwoPC_Vote_recvd)cout<<"BatchRequest2 vote 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        if(breq->TwoPC_Commit_recvd)cout<<"BatchRequest2 commit 2PC set. breq rc_rxn_id: "<<breq->rc_txn_id<<endl;
        fflush(stdout);
    }

    // Check if the message is valid.
    //validate_msg(breq);

    // Allocate transaction managers for all the transactions in the batch.
    set_txn_man_fields(breq, 0);

    //Check txn_man
    //printf("txn_man in process_batch: txn_id: %ld : rc_txn_id :%ld batch: %ld : THD: %ld\n",txn_man->get_txn_id(), 
    //txn_man->get_txn_id_RC(),txn_man->get_batch_id(), get_thd_id());
    //fflush(stdout);

#if TIMER_ON
    // The timer for this client batch stores the hash2 of last request.
    add_timer(breq, txn_man->get_hash2());
#endif

    // Send Prepare messages.
    txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
    double timepre = get_sys_clock() - cntime;
    INC_STATS(get_thd_id(), time_pre_prepare, timepre);

    // Only when BatchRequests message comes after some Prepare message.
    for (uint64_t i = 0; i < txn_man->info_prepare2.size(); i++)
    {
        // Decrement.
        uint64_t num_prep = txn_man->decr_prep_rsp_cnt2();
        if (num_prep == 0)
        {
            txn_man->set_prepared2();
            break;
        }
    }

    // If enough Prepare messages have already arrived.
    if (txn_man->is_prepared2())
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
        INC_STATS(get_thd_id(), time_prepare, timeprep);
        double timediff = get_sys_clock() - cntime;

        // Check if any Commit messages arrived before this BatchRequests message.
        for (uint64_t i = 0; i < txn_man->info_commit2.size(); i++)
        {
            uint64_t num_comm = txn_man->decr_commit_rsp_cnt2();
            if (num_comm == 0)
            {
                txn_man->set_committed2();
                break;
            }
        }

        // If enough Commit messages have already arrived.
        if (txn_man->is_committed2())
        {
#if TIMER_ON
            // End the timer for this client batch.
            server_timer->endTimer(txn_man->hash2);
#endif
            if(txn_man->get_cross_shard_txn())
            {
                if(txn_man->is_2PC_Commit_recvd()) cout<<"Choose shard for execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
                if (isRefCommittee())
                {
                    send_cross_shard_execute_msg();   
                }
                else if(isOtherShard())
                {
                    cout<<"Choose execute_msg for othershard txn_id: "<<txn_man->get_txn_id()<<endl;
                    if(!txn_man->is_2PC_Commit_recvd())
                    {
                        send_cross_shard_execute_msg();
                    }
                    else
                    {
                        cout<<"About to call send_execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
                        send_execute_msg();
                    }
                }
            }
            else
            {
            // Add this message to execute thread's queue.
                send_execute_msg();
            }

            // End the commit counter.
            INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
        }
    }
    else
    {
        // Although batch has not prepared, still some commit messages could have arrived.
        for (uint64_t i = 0; i < txn_man->info_commit2.size(); i++)
        {
            txn_man->decr_commit_rsp_cnt2();
        }
    }

    /*if(txn_man->is_2PC_Request_recvd())
        cout<<"Inside process_batch: 2PC request set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;*/
    if(txn_man->is_2PC_Vote_recvd())
        cout<<"Inside process_batch2: 2PC vote set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;
    if(txn_man->is_2PC_Commit_recvd())
        cout<<"Inside process_batch2: 2PC commit set in txn_man representing batch. rc_txn_id: "
        <<txn_man->get_txn_id_RC()<<endl;
    fflush(stdout);

    // Release this txn_man for other threads to use.
    bool ready = txn_man->set_ready();
    assert(ready);

    // UnSetting the ready for the txn id representing this batch.
    txn_man = get_transaction_manager(msg->txn_id, 0);
    while (true)
    {
        bool ready = txn_man->unset_ready();
        if (!ready)
        {
            continue;
        }
        else
        {
            break;
        }
    }

    return RCOK;
}
#endif

/**
 * Processes incoming Prepare message.
 *
 * This functions precessing incoming messages of type PBFTPrepMessage. If a replica 
 * received 2f identical Prepare messages from distinct replicas, then it creates 
 * and sends a PBFTCommitMessage to all the other replicas.
 *
 * @param msg Prepare message of type PBFTPrepMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_prep_msg(Message *msg)
{
       /*  cout << "PBFTPrepMessage: TID: " << msg->txn_id << " FROM: " << msg->return_node_id
        << " rc_txn_id: "<<txn_man->get_txn_id_RC()
        <<" prep_rsp_count: "<<txn_man->prep_rsp_cnt<<endl;
        cout << "is_2PC_Request_recvd: "<< txn_man->is_2PC_Request_recvd()<<endl;
        cout << "is_2PC_Vote_recvd: "<< txn_man->is_2PC_Vote_recvd()<<endl;
        fflush(stdout); */

    // Start the counter for prepare phase.
    if (txn_man->prep_rsp_cnt == 2 * g_min_invalid_nodes)
    {
        txn_man->txn_stats.time_start_prepare = get_sys_clock();
    }

    // Check if the incoming message is valid.
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
    validate_msg(pmsg);

    // Check if sufficient number of Prepare messages have arrived.
    if (prepared(pmsg))
    {
        /*if(txn_man->is_2PC_Request_recvd())
            cout<<"Inside process_pbft_prep: 2PC request set in txn_man representing batch. rc_txn_id: "
            <<txn_man->get_txn_id_RC()<<endl;*/
       /*  if(txn_man->is_2PC_Vote_recvd())
            cout<<"Inside process_pbft_prep: 2PC vote set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" txn_id_RC: "<<txn_man->get_txn_id_RC()<<endl;
        if(txn_man->is_2PC_Commit_recvd())
            cout<<"Inside process_pbft_prep: 2PC commit set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        fflush(stdout);
 */

        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        // End the prepare counter.
        INC_STATS(get_thd_id(), time_prepare, get_sys_clock() - txn_man->txn_stats.time_start_prepare);
    }

    return RCOK;
}

#if AHL
/**
 * Processes incoming Prepare message for the second local pbft in 2PC.
 *
 * This functions precessing incoming messages of type PBFTPrepMessage. If a replica 
 * received 2f identical Prepare messages from distinct replicas, then it creates 
 * and sends a PBFTCommitMessage to all the other replicas.
 *
 * @param msg Prepare message of type PBFTPrepMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_prep_msg2(Message *msg)
{
        cout << "PBFTPrepMessage2: TID: " << msg->txn_id << " FROM: " << msg->return_node_id
        << " rc_txn_id: "<<txn_man->get_txn_id_RC()
        <<" prep_rsp_count2: "<<txn_man->prep_rsp_cnt2<<endl;
        cout << "is_2PC_Request_recvd: "<< txn_man->is_2PC_Request_recvd()<<endl;
        cout << "is_2PC_Vote_recvd: "<< txn_man->is_2PC_Vote_recvd()<<endl;
        fflush(stdout);

    // Start the counter for prepare phase.
    if (txn_man->prep_rsp_cnt2 == 2 * g_min_invalid_nodes)
    {
        txn_man->txn_stats.time_start_prepare = get_sys_clock();
    }

    // Check if the incoming message is valid.
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
    validate_msg(pmsg);

    // Check if sufficient number of Prepare messages have arrived.
    if (prepared2(pmsg))
    {
        /*if(txn_man->is_2PC_Request_recvd())
            cout<<"Inside process_pbft_prep: 2PC request set in txn_man representing batch. rc_txn_id: "
            <<txn_man->get_txn_id_RC()<<endl;*/
        if(txn_man->is_2PC_Vote_recvd())
            cout<<"Inside process_pbft_prep2: 2PC vote set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" txn_id_RC: "<<txn_man->get_txn_id_RC()<<endl;
        if(txn_man->is_2PC_Commit_recvd())
            cout<<"Inside process_pbft_prep2: 2PC commit set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        fflush(stdout);

        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        // End the prepare counter.
        INC_STATS(get_thd_id(), time_prepare, get_sys_clock() - txn_man->txn_stats.time_start_prepare);
    }

    return RCOK;
}
#endif
/**
 * Checks if the incoming PBFTCommitMessage can be accepted.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f+1 messages are received it returns a true and 
 * sets the `is_committed` flag for furtue identification.
 *
 * @param msg PBFTCommitMessage.
 * @return bool True if the transactions of this batch can be executed.
 */
bool WorkerThread::committed_local(PBFTCommitMessage *msg)
{

    //cout<<"In commited local txnId : && txn-man commit count is"<<msg->txn_id<<txn_man->commit_rsp_cnt<<endl;
    /* if(txn_man->is_2PC_Vote_recvd()){
        //cout << "Check Commit: TID: " << txn_man->get_txn_id()<< " rc_txn_id: "<<txn_man->get_txn_id_RC()<< endl;
        fflush(stdout);
    } */

    // Once committed is set for this transaction, no further processing.
    if (txn_man->is_committed())
    {
        return false;
    }

    // If BatchRequests messages has not arrived, then hash is empty; return false.
    if (txn_man->get_hash().empty())
    {
        /* if(txn_man->is_2PC_Vote_recvd()){
            cout << "committed_local hash empty: " << txn_man->get_txn_id() << "\n";
            fflush(stdout);
        } */
        txn_man->info_commit.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            //cout << txn_man->get_hash() << " :: " << msg->hash << "\n";
            //cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
             //fflush(stdout);
            return false;
        }
    }

    uint64_t comm_cnt = txn_man->decr_commit_rsp_cnt();
    //if(txn_man->is_2PC_Vote_recvd()) cout<<"Commit count decremented:"<<comm_cnt<<" txn_id: "<<txn_man->get_txn_id()<<endl;
    if (comm_cnt == 0 && txn_man->is_prepared())
    {
        //if(txn_man->is_2PC_Vote_recvd()) cout<<"setting committed txn_id: "<<txn_man->get_txn_id()<<endl;
        txn_man->set_committed();
        return true;
    }

    return false;
}

#if AHL
/**
 * Checks if the incoming PBFTCommitMessage can be accepted in the second local pbft.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f+1 messages are received it returns a true and 
 * sets the `is_committed` flag for furtue identification.
 *
 * @param msg PBFTCommitMessage.
 * @return bool True if the transactions of this batch can be executed.
 */
bool WorkerThread::committed_local2(PBFTCommitMessage *msg)
{
    if(txn_man->is_2PC_Vote_recvd()){
        cout << "Check Commit2: TID: " << txn_man->get_txn_id()<< " rc_txn_id: "<<txn_man->get_txn_id_RC()<< endl;
        fflush(stdout);
    }

    // Once committed is set for this transaction, no further processing.
    if (txn_man->is_committed2())
    {
        return false;
    }

    // If BatchRequests messages has not arrived, then hash2 is empty; return false.
    if (txn_man->get_hash().empty())
    {
        if(txn_man->is_2PC_Vote_recvd()){
            cout << "committed_local2 hash2 empty: " << txn_man->get_txn_id() << "\n";
            fflush(stdout);
        }
        txn_man->info_commit2.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            //cout << txn_man->get_hash2() << " :: " << msg->hash << "\n";
            //cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
            //fflush(stdout);
            return false;
        }
    }

    uint64_t comm_cnt = txn_man->decr_commit_rsp_cnt2();
    if(txn_man->is_2PC_Vote_recvd()) cout<<"Commit2 count decremented:"<<comm_cnt<<" txn_id: "<<txn_man->get_txn_id()<<endl;
    if (comm_cnt == 0 && txn_man->is_prepared2())
    {
        if(txn_man->is_2PC_Vote_recvd()) cout<<"setting committed2 txn_id: "<<txn_man->get_txn_id()<<endl;
        txn_man->set_committed2();
        return true;
    }

    return false;
}
#endif
/**
 * Processes incoming Commit message.
 *
 * This functions precessing incoming messages of type PBFTCommitMessage. If a replica 
 * received 2f+1 identical Commit messages from distinct replicas, then it asks the 
 * execute-thread to execute all the transactions in this batch.
 *
 * @param msg Commit message of type PBFTCommitMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_commit_msg(Message *msg)
{
    //cout<<"In process pbft commit"<<endl;
/* 
    cout << "PBFTCommitMessage: TID " << msg->txn_id << " FROM: " << msg->return_node_id << 
        " batch_id :"<<msg->batch_id<< " rc_txn_id: "<<txn_man->get_txn_id_RC()
        <<" commit_rsp_count: "<<txn_man->commit_rsp_cnt<<endl;
        fflush(stdout);  */

    if (txn_man->commit_rsp_cnt == 2 * g_min_invalid_nodes+1)
    {
        txn_man->txn_stats.time_start_commit = get_sys_clock();
    }
PBFTCommitMessage *pcmsg = (PBFTCommitMessage *)msg;
#if AHL
    if(!(txn_man->is_2PC_Vote_recvd() || txn_man->is_2PC_Commit_recvd()))
    {
           // Check if message is valid.
        validate_msg(pcmsg);
        txn_man->add_commit_msg(pcmsg);
    }
#else
    validate_msg(pcmsg);
    txn_man->add_commit_msg(pcmsg);
#endif
     

    // Check if sufficient number of Commit messages have arrived.
    if (committed_local(pcmsg))
    {
#if TIMER_ON
        // End the timer for this client batch.
        server_timer->endTimer(txn_man->hash);
#endif
        /* if(txn_man->is_2PC_Request_recvd())
            cout<<"Inside process_pbft_commit: 2PC request set in txn_man representing batch. rc_txn_id: "
            <<txn_man->get_txn_id_RC()<<endl;
        if(txn_man->is_2PC_Vote_recvd())
            cout<<"Inside process_pbft_commit: 2PC vote set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        if(txn_man->is_2PC_Commit_recvd())
            cout<<"Inside process_pbft_commit: 2PC Commit set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        fflush(stdout); */


    //cout<<"txn_id: "<<txn_man->get_txn_id()<<" txnman->cs: "<<txn_man->get_cross_shard_txn()<<endl;
#if AHL
   if(txn_man->get_cross_shard_txn())
    {      
        if(txn_man->is_2PC_Commit_recvd()) cout<<"Choose shard for execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
        if (isRefCommittee())
        {
             send_cross_shard_execute_msg();
            
        }
        else if(isOtherShard())
        {
            cout<<"Choose execute_msg for othershard txn_id: "<<txn_man->get_txn_id()<<endl;
            if(!txn_man->is_2PC_Commit_recvd())
            {
                send_cross_shard_execute_msg();
            }
            else
            {
                cout<<"About to call send_execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
                send_execute_msg();
            }
            
        }    
    }   
    
    else
    {
       // Add this message to execute thread's queue.
        send_execute_msg();  
    }

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
    
#else
  // Add this message to execute thread's queue.
        send_execute_msg();

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
#endif
}
    return RCOK;
}

#if AHL
/**
 * Processes incoming Commit message for the second local pbft in 2PC.
 *
 * This functions precessing incoming messages of type PBFTCommitMessage. If a replica 
 * received 2f+1 identical Commit messages from distinct replicas, then it asks the 
 * execute-thread to execute all the transactions in this batch.
 *
 * @param msg Commit message of type PBFTCommitMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_commit_msg2(Message *msg)
{
        cout << "PBFTCommitMessage2: TID " << msg->txn_id << " FROM: " << msg->return_node_id << 
        " batch_id :"<<msg->batch_id<< " rc_txn_id: "<<txn_man->get_txn_id_RC()
        <<" commit_rsp_count2: "<<txn_man->commit_rsp_cnt2<<endl;
        fflush(stdout);

    if (txn_man->commit_rsp_cnt2 == 2 * g_min_invalid_nodes+1)
    {
        txn_man->txn_stats.time_start_commit = get_sys_clock();
    }

    PBFTCommitMessage *pcmsg = (PBFTCommitMessage *)msg;

    if(!(txn_man->is_2PC_Vote_recvd() || txn_man->is_2PC_Commit_recvd()))
    {
        // Check if message is valid.
        validate_msg(pcmsg);

        txn_man->add_commit_msg(pcmsg);
    }

    // Check if sufficient number of Commit messages have arrived.
    if (committed_local2(pcmsg))
    {
#if TIMER_ON
        // End the timer for this client batch.
        server_timer->endTimer(txn_man->hash2);
#endif
        /*if(txn_man->is_2PC_Request_recvd())
            cout<<"Inside process_pbft_commit: 2PC request set in txn_man representing batch. rc_txn_id: "
            <<txn_man->get_txn_id_RC()<<endl;*/
        if(txn_man->is_2PC_Vote_recvd())
            cout<<"Inside process_pbft_commit2: 2PC vote set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        if(txn_man->is_2PC_Commit_recvd())
            cout<<"Inside process_pbft_commit2: 2PC Commit set in txn_man representing batch. txn_id: "
            <<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;
        fflush(stdout);


    cout<<"txn_id: "<<txn_man->get_txn_id()<<" txnman->cs: "<<txn_man->get_cross_shard_txn()<<endl;
    if(txn_man->get_cross_shard_txn())
    {      
        if(txn_man->is_2PC_Commit_recvd()) cout<<"Choose shard for execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
        if (isRefCommittee())
        {
             send_cross_shard_execute_msg();
            
        }
        else if(isOtherShard())
        {
            cout<<"Choose execute_msg for othershard txn_id: "<<txn_man->get_txn_id()<<endl;
            if(!txn_man->is_2PC_Commit_recvd())
            {
                send_cross_shard_execute_msg();
            }
            else
            {
                cout<<"About to call send_execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
                send_execute_msg();
            }
            
        }       
    }
    else
    {
       // Add this message to execute thread's queue.
        send_execute_msg();  
    }

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
    }

    return RCOK;
}

//Methods for 2PC message processing
RC WorkerThread::process_request_2pc(Message *msg)
{
    Request_2PCBatch *req2PC = (Request_2PCBatch *)msg;

    printf("Request_2PCBatch local txn_id: %ld, THD: %ld :: From node: %ld :: rc_txn_id: %ld\n",req2PC->txn_id, get_thd_id(),msg->return_node_id ,req2PC->rc_txn_id);
    fflush(stdout);

    create_and_send_batchreq(req2PC, req2PC->txn_id);

    return RCOK;
}

RC WorkerThread::process_vote_2pc(Message *msg)
{
    Vote_2PC *vote2PC = (Vote_2PC *)msg;

    printf("Vote_2PC txn_id: %ld, THD: %ld :: From node: %ld :: rc_txn_id: %ld :: batch_id: %ld\n",
    vote2PC->txn_id, get_thd_id(),msg->return_node_id ,vote2PC->rc_txn_id,vote2PC->batch_id);
    printf("Vote_2PC txn_man txn_id: %ld :: rc_txn_id: %ld :: batch_id: %ld\n",
    txn_man->get_txn_id(), txn_man->get_txn_id_RC(), txn_man->get_batch_id());
    fflush(stdout);

    if(check_2pc_vote_recvd(vote2PC, txn_man)){
        send_batchreq_2PC(vote2PC, vote2PC->txn_id);
    }

    return RCOK;
}

RC WorkerThread::process_global_commit_2pc(Message *msg)
{
    Global_Commit_2PC *commit2PC = (Global_Commit_2PC *)msg;

    printf("Global_Commit_2PC txn_id: %ld, THD: %ld :: From node: %ld :: rc_txn_id: %ld :: batch_id: %ld\n",
    commit2PC->txn_id, get_thd_id(),msg->return_node_id ,commit2PC->rc_txn_id,commit2PC->batch_id);
    printf("Global_Commit_2PC txn_man txn_id: %ld :: rc_txn_id: %ld :: batch_id: %ld\n",
    txn_man->get_txn_id(), txn_man->get_txn_id_RC(), txn_man->get_batch_id());
    fflush(stdout);

    if(check_2pc_global_commit_recvd(commit2PC, txn_man)){
        cout << "Global_Commit_2PC Msg Batch id --> " << commit2PC->batch_id << endl;
        send_batchreq_2PC(commit2PC, txn_man->get_txn_id());
    }

    return RCOK;
}
#endif