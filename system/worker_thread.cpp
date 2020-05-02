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

void WorkerThread::send_key()
{
    // Send everyone the public key.
    for (uint64_t i = 0; i < g_node_cnt + g_client_node_cnt; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }
#if CRYPTO_METHOD_RSA || CRYPTO_METHOD_ED25519
        Message *msg = Message::create_message(KEYEX);
        KeyExchange *keyex = (KeyExchange *)msg;
        // The four first letters of the message set the type
#if CRYPTO_METHOD_RSA
        keyex->pkey = "RSA-" + g_public_key;
        cout << "Sending RSA: " << g_public_key << endl;
#elif CRYPTO_METHOD_ED25519
        keyex->pkey = "ED2-" + g_public_key;
        cout << "Sending ED25519: " << g_public_key << endl;
#endif
        fflush(stdout);

        keyex->pkeySz = keyex->pkey.size();
        keyex->return_node = g_node_id;
        //msg_queue.enqueue(get_thd_id(), keyex, i);

        vector<string> emptyvec;
        vector<uint64_t> dest;
        dest.push_back(i);
        msg_queue.enqueue(get_thd_id(), keyex, emptyvec, dest);

#endif

#if CRYPTO_METHOD_CMAC_AES
        Message *msgCMAC = Message::create_message(KEYEX);
        KeyExchange *keyexCMAC = (KeyExchange *)msgCMAC;
        keyexCMAC->pkey = "CMA-" + cmacPrivateKeys[i];
        keyexCMAC->pkeySz = keyexCMAC->pkey.size();
        keyexCMAC->return_node = g_node_id;
        //msg_queue.enqueue(get_thd_id(), keyexCMAC, i);

        msg_queue.enqueue(get_thd_id(), keyexCMAC, emptyvec, dest);
        dest.clear();

        cout << "Sending CMAC " << cmacPrivateKeys[i] << endl;
        fflush(stdout);
#endif
    }
}

void WorkerThread::setup()
{
    // Increment commonVar.
    batchMTX.lock();
    commonVar++;
    batchMTX.unlock();

    if (get_thd_id() == 0)
    {
        while (commonVar < g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt)
            ;

        send_init_done_to_all_nodes();

        send_key();
    }
    _thd_txn_id = 0;
}

void WorkerThread:: process(Message *msg)
{
    RC rc __attribute__((unused));

    switch (msg->get_rtype())
    {
    case KEYEX:
        rc = process_key_exchange(msg);
        break;
    case CL_BATCH:
        rc = process_client_batch(msg);
        break;
    case BATCH_REQ:
        {
            BatchRequests *breq_check = (BatchRequests *)msg;
            //Check which local pbft the batchrequest corresponds to
            if(breq_check->TwoPC_Vote_recvd) rc = process_batch2(msg);
            else rc = process_batch(msg);
        }
        break;
    case PBFT_CHKPT_MSG:
        rc = process_pbft_chkpt_msg(msg);
        break;
    case CROSS_SHARD_EXECUTE:
        cout<<"Received Cross Shard Execute"<<endl;
        process_cross_shard_execute_msg(msg);
        break;
    case EXECUTE_MSG:
        {
            cout<<"[PH] Received Execute Msg for txn ID: "<<msg->txn_id<<endl;
            TxnManager* txn_man_check = get_transaction_manager(msg->txn_id+1, 0);
            Array<uint64_t> shardsInvolved = txn_man_check->get_shards_involved();
            //cout<<"Size of shardsInvolved: "<<shardsInvolved.get_count()<<endl;
            //if(shardsInvolved.get_count()) cout<<"First entry: "<<shardsInvolved.get(0);
            if(isRefCommittee() && !shardsInvolved.contains(0))
            {
                rc = send_client_response(msg);
            }
            else
            {
                rc = process_execute_msg(msg);
            }
        break;
        }
#if VIEW_CHANGES
    case VIEW_CHANGE:
        rc = process_view_change_msg(msg);
        break;
    case NEW_VIEW:
        rc = process_new_view_msg(msg);
        break;
#endif
    case PBFT_PREP_MSG:
        {
            PBFTPrepMessage *pmsg_check = (PBFTPrepMessage *)msg;
            if(pmsg_check->first_local_pbft) rc = process_pbft_prep_msg(msg);
            else rc = process_pbft_prep_msg2(msg);
        }
            break;
    case PBFT_COMMIT_MSG:
        {
            PBFTCommitMessage *pcmsg_check = (PBFTCommitMessage *)msg;
            if(pcmsg_check->first_local_pbft) rc = process_pbft_commit_msg(msg);
            else rc = process_pbft_commit_msg2(msg);
        }
        break;
    case REQUEST_2PC:
        cout<<"Received 2PC Req in Node "<<g_node_id<<" from node:"<<msg->return_node_id<<endl;
        fflush(stdout);
        //Only process message if node is primary of a shard
        if(is_primary_node(get_thd_id(),g_node_id)) {
            rc = process_request_2pc(msg);
        }
        break;
    case VOTE_2PC:
        cout<<"Received 2PC Vote in Node "<<g_node_id<<" from node:"<<msg->return_node_id<<endl;
        //Only process message if node is primary of a shard
        if(is_primary_node(get_thd_id(),g_node_id)) {
            rc = process_vote_2pc(msg);
        }
        break;
    case GLOBAL_COMMIT_2PC:
        cout<<"Received 2PC Global Commit in Node "<<g_node_id<<endl;
        //Only process message if node is primary of a shard
        if(is_primary_node(get_thd_id(),g_node_id)) {
            rc = process_global_commit_2pc(msg);
        }
        break;
    default:
        printf("Msg: %d\n", msg->get_rtype());
        fflush(stdout);
        assert(false);
        break;
    }
}

bool WorkerThread::isRefCommittee()
{
    if(g_node_id<g_shard_size)
    {
        return true;    
    }
    return false;
}

bool WorkerThread::isOtherShard()
{
    if(g_node_id>=g_shard_size && g_node_id<g_node_cnt )
    {
        return true;    
    }
    return false;
}

RC WorkerThread::process_cross_shard_execute_msg(Message *msg)
{

        /* if(isOtherShard())
        {
            cout<<"2pc req flag set"<<txn_man->TwoPC_Request_recvd<<endl;
        } */
    //if current node is reference committee, phase -> Cross shard transaction received from client  
        if(isRefCommittee() && is_primary_node(get_thd_id(),g_node_id) && !txn_man->TwoPC_Request_recvd && !txn_man->TwoPC_Vote_recvd)
        {
           //create and send PREPARE_2PC_REQ message to the shards involved
           create_and_send_PREPARE_2PC(msg);
        }
        else if (isOtherShard() && is_primary_node(get_thd_id(),g_node_id) && txn_man->TwoPC_Request_recvd && !txn_man->TwoPC_Vote_recvd && !txn_man->TwoPC_Commit_recvd)
        {
            //cout<<"Checking if condtn"<<endl;
            create_and_send_Vote_2PC(msg);
        }
        else if (isRefCommittee() && g_node_id==0 && txn_man->TwoPC_Vote_recvd && !txn_man->TwoPC_Commit_recvd)
        {
            //cout<<"Checking if condtn"<<endl;
            create_and_send_global_commit(msg);
            //send_execute_msg();
        }
        else if(isRefCommittee() && g_node_id!=0 && txn_man->TwoPC_Vote_recvd && !txn_man->TwoPC_Commit_recvd)
        {
            cout<<"About to call send_execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
            send_execute_msg();
        }


    return RCOK;
}


RC WorkerThread::create_and_send_PREPARE_2PC(Message *msg)
{
    //cout<<"In send 2PC Req func"<<endl;
    //fflush(stdout);
    Message *mssg = Message::create_message(REQUEST_2PC);
    Request_2PCBatch *rmsg = (Request_2PCBatch *)mssg;
    rmsg->init();

    
    ExecuteMessage *emsg = (ExecuteMessage *)msg;
    rmsg->rc_txn_id = emsg->end_index;
    rmsg->batch_id = emsg->end_index;

    BatchRequests* breqdeepcopy = txn_man->get_primarybatch();
    for (uint64_t i=0; i<breqdeepcopy->requestMsg.size(); i++)
    {
        YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)breqdeepcopy->requestMsg[i];
        clqry->return_node = txn_man->client_id;
        rmsg->cqrySet.add(clqry);
               
    }
    vector<string> emptyvec;
	vector<uint64_t> dest;
    Array<uint64_t> shardsInvolved = txn_man->get_shards_involved();

    //populating destination array to send to invloved shards
    for (uint64_t i=0; i<shardsInvolved.size(); i++)
        {
            if(shardsInvolved[i]==0)//to make sure reference comittee doesnt send to itself
            {
                continue;
            }
            
            for(uint64_t j=shardsInvolved[i]*g_shard_size; j<(shardsInvolved[i]*g_shard_size)+g_shard_size; j++)
            {
                dest.push_back(j);           
            }             
        }  
        

    printf("Sending Request_2PCBatch: TID:%ld : THD: %ld\n",rmsg->rc_txn_id, get_thd_id());
    //fflush(stdout);

    //enqueue to msg_queue
	msg_queue.enqueue(get_thd_id(), rmsg, emptyvec, dest);
	dest.clear();

    return RCOK;  
}

RC WorkerThread::create_and_send_Vote_2PC(Message *msg)
{
    //cout<<"In create and send Vote 2PC func"<<endl;
    Message *mssg = Message::create_message(VOTE_2PC);
    Vote_2PC *vmsg = (Vote_2PC *)mssg;
    vmsg->init();

    //Vote_2PC should assign txn_id of Reference Committee which is rc_txn_id/batch_id of shards.
    vmsg->txn_id = txn_man->get_batch_id();
    vmsg->rc_txn_id = 0;
    vmsg->batch_id = 0;


    BatchRequests* breqdeepcopy = txn_man->get_primarybatch();
    for (uint64_t i=0; i<breqdeepcopy->requestMsg.size(); i++)
    {
        YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)breqdeepcopy->requestMsg[i];
        clqry->return_node = txn_man->client_id;
        vmsg->cqrySet.add(clqry);
               
    }

    vector<string> emptyvec;
	vector<uint64_t> dest;
        
    //populating destination array to send to all nodes of reference committee
    for (uint64_t i=0; i<g_shard_size; i++)
        {
            dest.push_back(i);           
        }

    printf("Sending Vote: ");             
        
    //enqueue to msg_queue
	msg_queue.enqueue(get_thd_id(), vmsg, emptyvec, dest);
	dest.clear();

    return RCOK;  
}

RC WorkerThread::create_and_send_global_commit(Message *msg)
{
    cout<<"[PH] In create and send Global Commit func"<<endl;
    Message *mssg = Message::create_message(GLOBAL_COMMIT_2PC);
    Global_Commit_2PC *gmsg = (Global_Commit_2PC *)mssg;
    gmsg->init();

    //GLobal Commit message should assign txn_id of Reference Committee which is rc_txn_id/batch_id of shards.
    gmsg->txn_id = txn_man->get_txn_id();
    gmsg->rc_txn_id = txn_man->get_txn_id();
    gmsg->batch_id = txn_man->get_txn_id();

    cout<<"[PH] Txn man in create and send global, txn_id"<<&txn_man<<"  "<<txn_man->get_txn_id()<<" address:"<<&txn_man<<endl;

    BatchRequests* breqdeepcopy = txn_man->get_primarybatch();
    for (uint64_t i=0; i<breqdeepcopy->requestMsg.size(); i++)
    {
        YCSBClientQueryMessage *clqry = (YCSBClientQueryMessage *)breqdeepcopy->requestMsg[i];
        clqry->return_node = txn_man->client_id;
        gmsg->cqrySet.add(clqry);
               
    }

    Array<uint64_t> shardsInvolved = txn_man->get_shards_involved();

    vector<string> emptyvec;
	vector<uint64_t> dest;

    //populating destination array to send to invloved shards
    for (uint64_t i=0; i<shardsInvolved.size(); i++)
        {
            if(shardsInvolved[i]==0)//to make sure reference comittee doesnt send to itself
            {
                continue;
            }
            
            for(uint64_t j=shardsInvolved[i]*g_shard_size; j<(shardsInvolved[i]*g_shard_size)+g_shard_size; j++)
            {
                dest.push_back(j);           
            }             
        }  
        
    cout<<"Sending GlobalCommit"<<endl;
    //enqueue to msg_queue
	msg_queue.enqueue(get_thd_id(), gmsg, emptyvec, dest);
	dest.clear();

    cout<<"About to call send_execute_msg txn_id: "<<txn_man->get_txn_id()<<endl;
    send_execute_msg();

    return RCOK;  
}





RC WorkerThread::process_key_exchange(Message *msg)
{
    KeyExchange *keyex = (KeyExchange *)msg;

    string algorithm = keyex->pkey.substr(0, 4);
    keyex->pkey = keyex->pkey.substr(4, keyex->pkey.size() - 4);
    //cout << "Algo: " << algorithm << " :: " <<keyex->return_node << endl;
    //cout << "Storing the key: " << keyex->pkey << " ::size: " << keyex->pkey.size() << endl;
    fflush(stdout);

#if CRYPTO_METHOD_CMAC_AES
    if (algorithm == "CMA-")
    {
        cmacOthersKeys[keyex->return_node] = keyex->pkey;
        receivedKeys[keyex->return_node]--;
    }
#endif

// When using ED25519 we create the verifier for this pkey.
// This saves some time during the verification
#if CRYPTO_METHOD_ED25519
    if (algorithm == "ED2-")
    {
        //cout << "Key length: " << keyex->pkey.size() << " for ED255: " << CryptoPP::ed25519PrivateKey::PUBLIC_KEYLENGTH << endl;
        g_pub_keys[keyex->return_node] = keyex->pkey;
        byte byteKey[CryptoPP::ed25519PrivateKey::PUBLIC_KEYLENGTH];
        copyStringToByte(byteKey, keyex->pkey);
        verifier[keyex->return_node] = CryptoPP::ed25519::Verifier(byteKey);
        receivedKeys[keyex->return_node]--;
    }

#elif CRYPTO_METHOD_RSA
    if (algorithm == "RSA-")
    {
        g_pub_keys[keyex->return_node] = keyex->pkey;
        receivedKeys[keyex->return_node]--;
    }
#endif

    bool sendReady = true;
    //Check if we have the keys of every node
    uint64_t totnodes = g_node_cnt + g_client_node_cnt;
    for (uint64_t i = 0; i < totnodes; i++)
    {
        if (receivedKeys[i] != 0)
        {
            sendReady = false;
        }
    }

    if (sendReady)
    {
        // Send READY to clients.
        for (uint64_t i = g_node_cnt; i < totnodes; i++)
        {
            Message *rdy = Message::create_message(READY);
            //msg_queue.enqueue(get_thd_id(), rdy, i);

            vector<string> emptyvec;
            vector<uint64_t> dest;
            dest.push_back(i);
            msg_queue.enqueue(get_thd_id(), rdy, emptyvec, dest);
            dest.clear();
        }

#if LOCAL_FAULT
        // Fail some node.
        for (uint64_t j = 1; j <= num_nodes_to_fail; j++)
        {
            uint64_t fnode = g_min_invalid_nodes + j;
            for (uint i = 0; i < g_send_thread_cnt; i++)
            {
                stopMTX[i].lock();
                stop_nodes[i].push_back(fnode);
                stopMTX[i].unlock();
            }
        }

        fail_nonprimary();
#endif
    }

    return RCOK;
}

void WorkerThread::release_txn_man(uint64_t txn_id, uint64_t batch_id)
{
    txn_table.release_transaction_manager(get_thd_id(), txn_id, batch_id);
}

TxnManager *WorkerThread::get_transaction_manager(uint64_t txn_id, uint64_t batch_id)
{
    TxnManager *tman = txn_table.get_transaction_manager(get_thd_id(), txn_id, batch_id);
    return tman;
}

/* Returns the id for the next txn. */
uint64_t WorkerThread::get_next_txn_id()
{
    uint64_t txn_id = get_batch_size() * next_set;
    return txn_id;
}

#if LOCAL_FAULT
/* This function focibly fails non-primary replicas. */
void WorkerThread::fail_nonprimary()
{
    if (g_node_id > g_min_invalid_nodes)
    {
        if (g_node_id - num_nodes_to_fail <= g_min_invalid_nodes)
        {
            uint64_t count = 0;
            while (true)
            {
                count++;
                if (count > 100000000)
                {
                    cout << "FAILING!!!";
                    fflush(stdout);
                    assert(0);
                }
            }
        }
    }
}

#endif

#if TIMER_ON
void WorkerThread::add_timer(Message *msg, string qryhash)
{
    char *tbuf = create_msg_buffer(msg);
    Message *deepMsg = deep_copy_msg(tbuf, msg);
    server_timer->startTimer(qryhash, deepMsg);
    delete_msg_buffer(tbuf);
}
#endif

#if VIEW_CHANGES
/*
Each non-primary replica continuously checks the timer for each batch. 
If there is a timeout then it initiates the view change. 
This requires sending a view change message to each replica.
*/
void WorkerThread::check_for_timeout()
{
    if (g_node_id != get_current_view(get_thd_id()) &&
        server_timer->checkTimer())
    {
        // Pause the timer to avoid causing further view changes.
        server_timer->pauseTimer();

        cout << "Begin Changing View" << endl;
        fflush(stdout);

        Message *msg = Message::create_message(VIEW_CHANGE);
        TxnManager *local_tman = get_transaction_manager(get_curr_chkpt(), 0);
        cout << "Initializing" << endl;
        fflush(stdout);

        ((ViewChangeMsg *)msg)->init(get_thd_id(), local_tman);

        cout << "Going to send" << endl;
        fflush(stdout);

        //send view change messages
        vector<string> emptyvec;
        vector<uint64_t> dest;
        for (uint64_t i = 0; i < g_node_cnt; i++)
        {
            //avoid sending msg to old primary
            if (i == get_current_view(get_thd_id()))
            {
                continue;
            }
            else if (i == g_node_id)
            {
                continue;
            }
            dest.push_back(i);
        }

        char *buf = create_msg_buffer(msg);
        Message *deepCMsg = deep_copy_msg(buf, msg);
        // Send to other replicas.
        msg_queue.enqueue(get_thd_id(), deepCMsg, emptyvec, dest);
        dest.clear();

        // process a message for itself
        deepCMsg = deep_copy_msg(buf, msg);
        work_queue.enqueue(get_thd_id(), deepCMsg, false);

        delete_msg_buffer(buf);
        Message::release_message(msg); // Releasing the message.

        cout << "Sent from here" << endl;
        fflush(stdout);
    }
}

/* 
In case there is a view change, all the threads would need a change of views.
*/
void WorkerThread::check_switch_view()
{
    uint64_t thd_id = get_thd_id();
    if (thd_id == 0)
    {
        return;
    }
    else
    {
        thd_id--;
    }

    uint32_t nchange = get_newView(thd_id);

    if (nchange)
    {
        cout << "Change: " << thd_id + 1 << "\n";
        fflush(stdout);

        set_current_view(thd_id + 1, get_current_view(thd_id + 1) + 1);
        set_newView(thd_id, false);
    }
}

/* This function causes the forced failure of the primary replica at a 
desired batch. */
void WorkerThread::fail_primary(Message *msg, uint64_t batch_to_fail)
{
    if (g_node_id == 0 && msg->txn_id > batch_to_fail)
    {
        uint64_t count = 0;
        while (true)
        {
            count++;
            if (count > 1000000000)
            {
                assert(0);
            }
        }
    }
}

void WorkerThread::store_batch_msg(BatchRequests *breq)
{
    char *bbuf = create_msg_buffer(breq);
    Message *deepCMsg = deep_copy_msg(bbuf, breq);
    storeBatch((BatchRequests *)deepCMsg);
    delete_msg_buffer(bbuf);
}

/*
The client forwarded its request to a non-primary replica. 
This maybe a potential case for a malicious primary.
Hence store the request, start timer and forward it to the primary replica.
*/
void WorkerThread::client_query_check(ClientQueryBatch *clbtch)
{
    //cout << "REQUEST: " << clbtch->return_node_id << "\n";
    //fflush(stdout);

    //start timer when client broadcasts an unexecuted message
    // Last request of the batch.
    YCSBClientQueryMessage *qry = clbtch->cqrySet[clbtch->batch_size - 1];
    add_timer(clbtch, calculateHash(qry->getString()));

    // Forward to the primary.
    vector<string> emptyvec;
    emptyvec.push_back(clbtch->signature); // Sign the message.
    vector<uint64_t> dest;
    dest.push_back(get_current_view(get_thd_id()));

    char *tbuf = create_msg_buffer(clbtch);
    Message *deepCMsg = deep_copy_msg(tbuf, clbtch);
    msg_queue.enqueue(get_thd_id(), deepCMsg, emptyvec, dest);
    delete_msg_buffer(tbuf);
}

/****************************************/
/* Functions for handling view changes. */
/****************************************/

RC WorkerThread::process_view_change_msg(Message *msg)
{
    cout << "PROCESS VIEW CHANGE "
         << "\n";
    fflush(stdout);

    ViewChangeMsg *vmsg = (ViewChangeMsg *)msg;

    // Ignore the old view messages or those delivered after view change.
    if (vmsg->view <= get_current_view(get_thd_id()))
    {
        return RCOK;
    }

    //assert view is correct
    assert(vmsg->view == ((get_current_view(get_thd_id()) + 1) % g_node_cnt));

    //cout << "validating view change message" << endl;
    //fflush(stdout);

    if (!vmsg->addAndValidate(get_thd_id()))
    {
        //cout << "waiting for more view change messages" << endl;
        return RCOK;
    }

    //cout << "executing view change message" << endl;
    //fflush(stdout);

    // Only new primary performs rest of the actions.
    if (g_node_id == vmsg->view)
    {
        //cout << "New primary rules!" << endl;
        //fflush(stdout);

        set_current_view(get_thd_id(), g_node_id);

        // Reset the views for different threads.
        uint64_t total_thds = g_batch_threads + g_rem_thread_cnt + 2;
        for (uint64_t i = 0; i < total_thds; i++)
        {
            set_newView(i, true);
        }

        // Need to ensure that all the threads of the new primary are ready.
        uint64_t count = 0;
        bool lflag[total_thds] = {false};
        while (true)
        {
            for (uint64_t i = 0; i < total_thds; i++)
            {
                bool nchange = get_newView(i);
                if (!nchange && !lflag[i])
                {
                    count++;
                    lflag[i] = true;
                }
            }

            if (count == total_thds)
            {
                break;
            }
        }

        Message *newViewMsg = Message::create_message(NEW_VIEW);
        NewViewMsg *nvmsg = (NewViewMsg *)newViewMsg;
        nvmsg->init(get_thd_id());

        cout << "New primary is ready to fire" << endl;
        fflush(stdout);

        // Adding older primary to list of failed nodes.
        for (uint i = 0; i < g_send_thread_cnt; i++)
        {
            stopMTX[i].lock();
            stop_nodes[i].push_back(g_node_id - 1);
            stopMTX[i].unlock();
        }

        //send new view messages
        vector<string> emptyvec;
        vector<uint64_t> dest;
        for (uint64_t i = 0; i < g_node_cnt; i++)
        {
            if (i == g_node_id)
            {
                continue;
            }
            dest.push_back(i);
        }

        char *buf = create_msg_buffer(nvmsg);
        Message *deepCMsg = deep_copy_msg(buf, nvmsg);
        msg_queue.enqueue(get_thd_id(), deepCMsg, emptyvec, dest);
        dest.clear();

        delete_msg_buffer(buf);
        Message::release_message(newViewMsg);

        // Remove all the ViewChangeMsgs.
        clearAllVCMsg();

        // Setting up the next txn id.
        set_next_idx(curr_next_index() / get_batch_size());

        set_expectedExecuteCount(curr_next_index() + get_batch_size() - 2);
        cout << "expectedExeCount = " << expectedExecuteCount << endl;
        fflush(stdout);

        // Start the re-directed requests.
        Timer *tmap;
        Message *msg;
        for (uint64_t i = 0; i < server_timer->timerSize(); i++)
        {
            tmap = server_timer->fetchPendingRequests(i);
            msg = tmap->get_msg();

            //YCSBClientQueryMessage *yc = ((ClientQueryBatch *)msg)->cqrySet[0];

            //cout << "MSG: " << yc->return_node << " :: Key: " << yc->requests[0]->key << "\n";
            //fflush(stdout);

            char *buf = create_msg_buffer(msg);
            Message *deepCMsg = deep_copy_msg(buf, msg);

            // Assigning an identifier to the batch.
            deepCMsg->txn_id = get_and_inc_next_idx();
            work_queue.enqueue(get_thd_id(), deepCMsg, false);
            delete_msg_buffer(buf);
        }

        // Clear the timer.
        server_timer->removeAllTimers();
    }

    return RCOK;
}

RC WorkerThread::process_new_view_msg(Message *msg)
{
    cout << "PROCESS NEW VIEW " << msg->txn_id << "\n";
    fflush(stdout);

    NewViewMsg *nvmsg = (NewViewMsg *)msg;
    if (!nvmsg->validate(get_thd_id()))
    {
        assert(0);
        return RCOK;
    }

    // Adding older primary to list of failed nodes.
    for (uint i = 0; i < g_send_thread_cnt; i++)
    {
        stopMTX[i].lock();
        stop_nodes[i].push_back(nvmsg->view - 1);
        stopMTX[i].unlock();
    }

    // Move to the next view.
    set_current_view(get_thd_id(), nvmsg->view);

    // Reset the views for different threads.
    uint64_t total_thds = g_batch_threads + g_rem_thread_cnt + 2;
    for (uint64_t i = 0; i < total_thds; i++)
    {
        set_newView(i, true);
    }

    // Need to ensure that all the threads of the new primary are ready.
    uint64_t count = 0;
    bool lflag[total_thds] = {false};
    while (true)
    {
        for (uint64_t i = 0; i < total_thds; i++)
        {
            bool nchange = get_newView(i);
            if (!nchange && !lflag[i])
            {
                count++;
                lflag[i] = true;
            }
        }

        if (count == total_thds)
        {
            break;
        }
    }

    //cout << "new primary changed view" << endl;
    //fflush(stdout);

    // Remove all the ViewChangeMsgs.
    clearAllVCMsg();

    // Setting up the next txn id.
    set_next_idx((curr_next_index() + 1) % get_batch_size());

    set_expectedExecuteCount(curr_next_index() + get_batch_size() - 2);

    // Clear the timer entries.
    server_timer->removeAllTimers();

    // Restart the timer.
    server_timer->resumeTimer();

    return RCOK;
}

#endif // VIEW_CHANGES

/**
 * Starting point for each worker thread.
 *
 * Each worker-thread created in the main() starts here. Each worker-thread is alive
 * till the time simulation is not done, and continuousy perform a set of tasks. 
 * Thess tasks involve, dequeuing a message from its queue and then processing it 
 * through call to the relevant function.
 */
RC WorkerThread::run()
{
    tsetup();
    printf("Running WorkerThread %ld\n", _thd_id);

    uint64_t agCount = 0, ready_starttime, idle_starttime = 0;

    // Setting batch (only relevant for batching threads).
    next_set = 0;

    while (!simulation->is_done())
    {
        txn_man = NULL;
        heartbeat();
        progress_stats();

#if VIEW_CHANGES
        // Thread 0 continously monitors the timer for each batch.
        if (get_thd_id() == 0)
        {
            check_for_timeout();
        }

        if (g_node_id != get_current_view(get_thd_id()))
        {
            check_switch_view();
        }
#endif

        // Dequeue a message from its work_queue.
        Message *msg = work_queue.dequeue(get_thd_id());
        if (!msg)
        {
            if (idle_starttime == 0)
                idle_starttime = get_sys_clock();
            continue;
        }
        if (idle_starttime > 0)
        {
            INC_STATS(_thd_id, worker_idle_time, get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }
        if(msg->rtype == GLOBAL_COMMIT_2PC)
        {
            cout<<"Global Commit 2PC dequeued"<<endl;
        }


        #if VIEW_CHANGES
        // Ensure that thread 0 of the primary never processes ClientQueryBatch.
        if (g_node_id == get_current_view(get_thd_id()))
        {
            if (msg->rtype == CL_BATCH)
            {
                if (get_thd_id() == 0)
                {
                    work_queue.enqueue(get_thd_id(), msg, false);
                    continue;
                }
            }
        }
#endif

        // Remove redundant messages.
        if (exception_msg_handling(msg))
        {
            continue;
        }

        // Based on the type of the message, we try acquiring the transaction manager.
        if (msg->rtype != BATCH_REQ && msg->rtype != CL_BATCH && msg->rtype != EXECUTE_MSG && msg->rtype != REQUEST_2PC)
        {

            if(msg->rtype == GLOBAL_COMMIT_2PC){
                //if(batch_id_directory.exists(msg->get_batch_id())) 
                cout<<"Retrieving batch_id to txn_id mapping batch_id: "<<msg->get_batch_id()<<" txn_id: "
                <<batch_id_directory.get(msg->get_batch_id()) * get_batch_size() + get_batch_size() - 1<<endl;
                txn_man = get_transaction_manager(batch_id_directory.get(msg->get_batch_id()) * get_batch_size() + get_batch_size() - 1, 0);
            }
            else{
                txn_man = get_transaction_manager(msg->txn_id, 0);
            }

            ready_starttime = get_sys_clock();
            bool ready = txn_man->unset_ready();
            if (!ready)
            {
                //cout << "Placing: Txn: " << msg->txn_id << " Type: " << msg->rtype << "\n";
                //fflush(stdout);
                // Return to work queue, end processing
                work_queue.enqueue(get_thd_id(), msg, true);
                continue;
            }
            txn_man->register_thread(this);
        }

        // Th execut-thread only picks up the next batch for execution.
        if (msg->rtype == EXECUTE_MSG)
        {
            if (msg->txn_id != get_expectedExecuteCount())
            {
                // Return to work queue.
                agCount++;
                work_queue.enqueue(get_thd_id(), msg, true);
                continue;
            }
        }

        
        process(msg);

        ready_starttime = get_sys_clock();
        if (txn_man)
        {
            bool ready = txn_man->set_ready();
            if (!ready)
            {
                cout << "FAIL: " << txn_man->get_txn_id() << " :: RT: " << msg->rtype << "\n";
                fflush(stdout);
                assert(ready);
            }
        }

        // delete message
        ready_starttime = get_sys_clock();
        Message::release_message(msg);

        INC_STATS(get_thd_id(), worker_release_msg_time, get_sys_clock() - ready_starttime);
    }
    printf("FINISH: %ld\n", agCount);
    fflush(stdout);

    return FINISH;
}

RC WorkerThread::init_phase()
{
    RC rc = RCOK;
    return rc;
}

bool WorkerThread::is_cc_new_timestamp()
{
    return false;
}

/**
 * This function sets up the required fields of the txn manager.
 *
 * @param clqry One Client Transaction (or Query).
*/
void WorkerThread::init_txn_man(YCSBClientQueryMessage *clqry)
{
    txn_man->client_id = clqry->return_node;
    txn_man->client_startts = clqry->client_startts;
    uint64_t shard_list_size = clqry->shards_involved.size();
    //Initialize shard list with size of shard list in message
    txn_man->init_shards_involved(shard_list_size);
    //Copy transaction list from message to transaction in TxnManager
    for(uint64_t i=0;i<clqry->shards_involved.size();i++){
        txn_man->set_shards_involved(clqry->shards_involved.get(i));
    }

    txn_man->txn->cross_shard_txn = false;
    //Check if request is a cross shard transaction
    if(clqry->cross_shard_txn){
        //Set cross shard transaction bool of transaction
        txn_man->set_cross_shard_txn();
        //Vote count expected by reference committee should be number of shards involved in the txn
        txn_man->TwoPC_Vote_cnt = shard_list_size;
    }

    YCSBQuery *query = (YCSBQuery *)(txn_man->query);
    for (uint64_t i = 0; i < clqry->requests.size(); i++)
    {
        ycsb_request *req = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request));
        req->key = clqry->requests[i]->key;
        req->value = clqry->requests[i]->value;
        query->requests.add(req);
    }
}

/**
 * Create an message of type ExecuteMessage, to notify the execute-thread that this 
 * batch of transactions are ready to be executed. This message is placed in one of the 
 * several work-queues for the execute-thread.
 */
void WorkerThread::send_execute_msg()
{
    Message *tmsg = Message::create_message(txn_man, EXECUTE_MSG);
    work_queue.enqueue(get_thd_id(), tmsg, false);
}

/**
 * Create an message of type CrossShardExecuteMessage, to send cross shard messages to other shards
 */
void WorkerThread::send_cross_shard_execute_msg()
{
    Message *tmsg = Message::create_message(txn_man, CROSS_SHARD_EXECUTE);
    work_queue.enqueue(get_thd_id(), tmsg, false); 
}

/**
 * Execute transactions and send client response.
 *
 * This function is only accessed by the execute-thread, which executes the transactions
 * in a batch, in order. Note that the execute-thread has several queues, and at any 
 * point of time, the execute-thread is aware of which is the next transaction to 
 * execute. Hence, it only loops on one specific queue.
 *
 * @param msg Execute message that notifies execution of a batch.
 * @ret RC
 */
RC WorkerThread::process_execute_msg(Message *msg)
{
    cout << "EXECUTE " << msg->txn_id << " :: " << get_thd_id() <<"\n";
    fflush(stdout);

    uint64_t ctime = get_sys_clock();


    // This message uses txn man of index calling process_execute.
    Message *rsp = Message::create_message(CL_RSP);
    ClientResponseMessage *crsp = (ClientResponseMessage *)rsp;
    crsp->init();

    ExecuteMessage *emsg = (ExecuteMessage *)msg;

    // Execute transactions in a shot
    uint64_t i;
    for (i = emsg->index; i < emsg->end_index - 4; i++)
    {
        //cout << "i: " << i << " :: next index: " << g_next_index << "\n";
        //fflush(stdout);

        TxnManager *tman = get_transaction_manager(i, 0);

        inc_next_index();

        // Execute the transaction
        tman->run_txn();

        // Commit the results.
        tman->commit();

        crsp->copy_from_txn(tman);
    }

    // Transactions (**95 - **98) of the batch.
    // We process these transactions separately, as we want to
    // ensure that their txn man are not held by some other thread.
    for (; i < emsg->end_index; i++)
    {
        TxnManager *tman = get_transaction_manager(i, 0);
        while (true)
        {
            bool ready = tman->unset_ready();
            if (!ready)
            {
                continue;
            }
            else
            {
                break;
            }
        }

        inc_next_index();

        // Execute the transaction
        tman->run_txn();

        // Commit the results.
        tman->commit();

        crsp->copy_from_txn(tman);

        // Making this txn man available.
        bool ready = tman->set_ready();
        assert(ready);
    }

    // Last Transaction of the batch.
    if(isOtherShard())txn_man = get_transaction_manager(i, 0);
    else txn_man = get_transaction_manager(i, 0);
    cout<<" [PH] Received Execute message and in process_execute_msg of last txn_id: "<<txn_man->get_txn_id()
    <<" 2pc request received: "<<txn_man->is_2PC_Request_recvd()<<endl;


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

    inc_next_index();

    // Execute the transaction
    txn_man->run_txn();

#if ENABLE_CHAIN
    // Add the block to the blockchain.
    cout<<"Before add_block txn_id: "<<txn_man->get_txn_id();
    BlockChain->add_block(txn_man);
    cout<<"After add_block txn_id: "<<txn_man->get_txn_id();
#endif

    // Commit the results.
    txn_man->commit();

    if((txn_man->get_cross_shard_txn() && isRefCommittee()) || !txn_man->get_cross_shard_txn())
    {   crsp->copy_from_txn(txn_man);

        vector<string> emptyvec;
        vector<uint64_t> dest;
        dest.push_back(txn_man->client_id);
        msg_queue.enqueue(get_thd_id(), crsp, emptyvec, dest);
        dest.clear();
    }

    INC_STATS(_thd_id, tput_msg, 1);
    INC_STATS(_thd_id, msg_cl_out, 1);

    // Check and Send checkpoint messages.
    send_checkpoints(txn_man->get_txn_id());
    cout<<"Successful execute1: txn_id: "<<txn_man->get_txn_id()<<endl;

    // Setting the next expected prepare message id.
    set_expectedExecuteCount(get_batch_size() + msg->txn_id);
    cout<<"Successful execute2: txn_id: "<<txn_man->get_txn_id()<<endl;

    // End the execute counter.
    INC_STATS(get_thd_id(), time_execute, get_sys_clock() - ctime);
    cout<<"[PH] Successful execute3: txn_id: "<<txn_man->get_txn_id()<<endl;
    return RCOK;
}


/**
 * Sending Client Response.
 *
 * This function is used by the nodes in the Reference Committee when 
 * the Ref. committee is not involved in the cross shard transaction, 
 * it only sends client repsonse, doesnt execute the transaction. 
 *
 * @param msg Execute message that notifies execution of a batch.
 * @ret RC
 */
RC WorkerThread:: send_client_response(Message *msg)
{
    //cout << "EXECUTE " << msg->txn_id << " :: " << get_thd_id() <<"\n";
    //fflush(stdout);

    // This message uses txn man of index calling process_execute.
    Message *rsp = Message::create_message(CL_RSP);
    ClientResponseMessage *crsp = (ClientResponseMessage *)rsp;
    crsp->init();

    ExecuteMessage *emsg = (ExecuteMessage *)msg;

    // Execute transactions in a shot
    uint64_t i;
    for (i = emsg->index; i < emsg->end_index - 4; i++)
    {
        //cout << "i: " << i << " :: next index: " << g_next_index << "\n";
        //fflush(stdout);

        TxnManager *tman = get_transaction_manager(i, 0);
        crsp->copy_from_txn(tman);
    }

    // Transactions (**95 - **98) of the batch.
    // We process these transactions separately, as we want to
    // ensure that their txn man are not held by some other thread.
    for (; i < emsg->end_index; i++)
    {
        TxnManager *tman = get_transaction_manager(i, 0);
        while (true)
        {
            bool ready = tman->unset_ready();
            if (!ready)
            {
                continue;
            }
            else
            {
                break;
            }
        }

        crsp->copy_from_txn(tman);

        // Making this txn man available.
        bool ready = tman->set_ready();
        assert(ready);
    }

    // Last Transaction of the batch.
    txn_man = get_transaction_manager(i, 0);
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

    crsp->copy_from_txn(txn_man);

    vector<string> emptyvec;
    vector<uint64_t> dest;
    dest.push_back(txn_man->client_id);
    msg_queue.enqueue(get_thd_id(), crsp, emptyvec, dest);
    dest.clear();

    INC_STATS(_thd_id, tput_msg, 1);
    INC_STATS(_thd_id, msg_cl_out, 1);

    return RCOK;
}








/**
 * This function helps in periodically sending out CheckpointMessage. At present these
 * messages are including just including information about first and last txn of the 
 * batch but later we should include a digest. Further, a checkpoint is only sent after
 * transaction id is a multiple of a config.h parameter.
 *
 * @param txn_id Transaction identifier of the last transaction in the batch..
 * @ret RC
 */
void WorkerThread::send_checkpoints(uint64_t txn_id)
{
    if ((txn_id + 1) % txn_per_chkpt() == 0)
    {
        TxnManager *tman =
            txn_table.get_transaction_manager(get_thd_id(), txn_id, 0);
        tman->send_checkpoint_msgs();
    }
}

/**
 * Checkpoint and Garbage collection.
 *
 * This function waits for 2f+1 messages to mark a checkpoint. Due to different 
 * processing speeds of the replicas, it is possible that a replica may receive 
 * CheckpointMessage from other replicas even before it has finished executing thst 
 * transaction. Hence, we need to be careful when to perform garbage collection.
 * Further, note that the CheckpointMessage messages are handled by a separate thread.
 *
 * @param msg CheckpointMessage corresponding to a checkpoint.
 * @ret RC
 */
RC WorkerThread::process_pbft_chkpt_msg(Message *msg)
{
    CheckpointMessage *ckmsg = (CheckpointMessage *)msg;

    // Check if message is valid.
    validate_msg(ckmsg);

    // If this checkpoint was already accessed, then return.
    if (txn_man->is_chkpt_ready())
    {
        return RCOK;
    }
    else
    {
        uint64_t num_chkpt = txn_man->decr_chkpt_cnt();
        // If sufficient number of messages received, then set the flag.
        if (num_chkpt == 0)
        {
            txn_man->set_chkpt_ready();
        }
        else
        {
            return RCOK;
        }
    }

    // Also update the next checkpoint to the identifier for this message,
    set_curr_chkpt(msg->txn_id);

    // Now we determine what all transaction managers can we release.
    uint64_t del_range = 0;
    if (curr_next_index() > get_curr_chkpt())
    {
        del_range = get_curr_chkpt() - get_batch_size();
    }
    else
    {
        if (curr_next_index() > get_batch_size())
        {
            del_range = curr_next_index() - get_batch_size();
        }
    }

    //printf("Chkpt: %ld :: LD: %ld :: Del: %ld\n",msg->get_txn_id(), get_last_deleted_txn(), del_range);
    //fflush(stdout);

    // Release Txn Managers.
    for (uint64_t i = get_last_deleted_txn(); i < del_range; i++)
    {
        //cout<<"For chkpt txn_id: "<<msg->txn_id<<" Releasing txn_id: "<<i<<" chkpt msg batch_id: "<<msg->batch_id<<endl;
        if(i%g_batch_size==g_batch_size-1 && isOtherShard()) release_txn_man(i, 0);
        else release_txn_man(i, 0);
        inc_last_deleted_txn();

#if ENABLE_CHAIN
        if ((i + 1) % get_batch_size() == 0)
        {
            BlockChain->remove_block(i);
        }
#endif
    }

#if VIEW_CHANGES
    removeBatch(del_range);
#endif

    return RCOK;
}

/* Specific handling for certain messages. If no handling then return false. */
bool WorkerThread::exception_msg_handling(Message *msg)
{
    if (msg->rtype == KEYEX)
    {
        // Key Exchange message needs to pass directly.
        process(msg);
        Message::release_message(msg);
        return true;
    }

    // Release Messages that arrive after txn completion, except obviously
    // CL_BATCH and 2PC messages as they are never late.
    if (msg->rtype != CL_BATCH && msg->rtype != REQUEST_2PC && msg->rtype != VOTE_2PC && msg->rtype != GLOBAL_COMMIT_2PC)
    {
        if (msg->rtype != PBFT_CHKPT_MSG)
        {
            if (msg->txn_id <= curr_next_index())
            {
                //cout << "Extra Msg: " << msg->txn_id << "msg rtype: "<< msg->rtype <<endl;
                //fflush(stdout);
                Message::release_message(msg);
                return true;
            }
        }
        else
        {
            if (msg->txn_id <= get_curr_chkpt())
            {
                Message::release_message(msg);
                return true;
            }
        }
    }

    return false;
}

/** UNUSED */
void WorkerThread::algorithm_specific_update(Message *msg, uint64_t idx)
{
    // Can update any field, if required for a different consensus protocol.
}

/**
 * This function is used by the non-primary or backup replicas to create and set 
 * transaction managers for each transaction part of the BatchRequests message sent by
 * the primary replica.
 *
 * @param breq Batch of transactions as a BatchRequests message.
 * @param bid Another dimensional identifier to support more transaction managers.
 */
void WorkerThread::set_txn_man_fields(BatchRequests *breq, uint64_t bid)
{
    if(!(breq->TwoPC_Vote_recvd || breq->TwoPC_Commit_recvd))
    {
        for (uint64_t i = 0; i < get_batch_size(); i++)
        {
            //If Request_2PC received in shard instead of Client_Batch, create last txn_man of batch with 2PC info
            if(i==get_batch_size()-1 && breq->rc_txn_id!=0){
                txn_man = get_transaction_manager(breq->index[i], breq->rc_txn_id);
                batch_id_directory.add(breq->rc_txn_id,breq->index[i]/get_batch_size());
            }
            else 
                txn_man = get_transaction_manager(breq->index[i], bid);

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

            txn_man->register_thread(this);
            txn_man->return_id = breq->return_node_id;

            // Fields that need to updated according to the specific algorithm.
            algorithm_specific_update(breq, i);

            init_txn_man(breq->requestMsg[i]);

            bool ready = txn_man->set_ready();
            assert(ready);
        }
    }

    if(breq->TwoPC_Vote_recvd || breq->TwoPC_Commit_recvd){
        if(batch_id_directory.exists(breq->get_batch_id())) cout<<"Retrieving batch_id to txn_id mapping batch_id: "<<breq->batch_id<<" txn_id: "
        <<batch_id_directory.get(breq->batch_id) * get_batch_size() + get_batch_size() - 1<<endl;
        txn_man = get_transaction_manager(breq->txn_id + 2, 0);
    }

    // We need to unset txn_man again for last txn in the batch.
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

    if(!breq->TwoPC_Vote_recvd) txn_man->set_hash(breq->hash);
    else txn_man->set_hash2(breq->hash);
    
    //Set 2PC state info accoring to 2PC flag set in BatchRequest
    if(breq->TwoPC_Request_recvd){
        txn_man->set_2PC_Request_recvd();
        txn_man->set_batch_id(breq->rc_txn_id);
        txn_man->set_txn_id_RC(breq->rc_txn_id);

        /*cout<<"set_txn_man_fields from BatchRequest: "<<txn_man->get_txn_id()
        <<" rc_txn_id: "<<txn_man->get_txn_id_RC()
        <<" txn_man hash: "<<txn_man->get_hash()<<endl;*/
    }
    if(breq->TwoPC_Vote_recvd) {
        txn_man->set_2PC_Request_recvd();
        txn_man->set_2PC_Vote_recvd();
    }
    if(breq->TwoPC_Commit_recvd) {
        txn_man->set_2PC_Vote_recvd();
        txn_man->set_2PC_Commit_recvd();
    }
}

/**
 * This function is used by the primary replicas to create and set 
 * transaction managers for each transaction part of the ClientQueryBatch message sent 
 * by the client. Further, to ensure integrity a hash of the complete batch is 
 * generated, which is also used in future communication.
 *
 * @param msg Batch of transactions as a ClientQueryBatch message.
 * @param tid Identifier for the first transaction of the batch.
 */
void WorkerThread::create_and_send_batchreq(ClientQueryBatch *msg, uint64_t tid)
{
    //cout<<"In create_and_send_batchreq for tid: "<<tid<<endl;
    // Creating a new BatchRequests Message.
    Message *bmsg = Message::create_message(BATCH_REQ);
    BatchRequests *breq = (BatchRequests *)bmsg;
    breq->init(get_thd_id());

    // Starting index for this batch of transactions.
    next_set = tid;

    // String of transactions in a batch to generate hash.
    string batchStr;

    // Allocate transaction manager for all the requests in batch.
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        uint64_t txn_id = get_next_txn_id() + i;

        //cout << "Txn: " << txn_id << " :: Thd: " << get_thd_id() << "\n";
        //fflush(stdout);

        //If Request_2PC received instead of Client_Batch, create last txn_man of batch with 2PC info
        if(i == get_batch_size()-1 && msg->rtype == REQUEST_2PC)
        {
            Request_2PCBatch *reqmsg = (Request_2PCBatch *)msg;
            txn_man = get_transaction_manager(txn_id, reqmsg->rc_txn_id);
            //Add mapping
            batch_id_directory.add(reqmsg->rc_txn_id, txn_id/get_batch_size());
            //cout<<"Set txn_man txn_id: "<<txn_man->get_txn_id()<<" rc_txn_id: "<<txn_man->get_txn_id_RC()
            //<<" 2pcRequestrecv: "<<txn_man->is_2PC_Request_recvd()<<endl;
        }
        else 
            txn_man = get_transaction_manager(txn_id, 0);

        // Unset this txn man so that no other thread can concurrently use.
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

        txn_man->register_thread(this);
        txn_man->return_id = msg->return_node;

        // Fields that need to updated according to the specific algorithm.
        algorithm_specific_update(msg, i);

        init_txn_man(msg->cqrySet[i]);

        //Check if cross_shard_txn is set
        /* if(msg->cqrySet[i]->cross_shard_txn){
            cout<<"Inter Shard Flag set\n";
            fflush(stdout);
        } */

        //Print shard list
        /* cout<<"List of shards in transaction ID:"<<txn_id<<"\n";
        fflush(stdout);
        for(uint64_t j=0;j<msg->cqrySet[i]->shards_involved.size();j++){
            cout<<msg->cqrySet[i]->shards_involved[j]<<"\n";
            fflush(stdout);
        } */

        /* if(msg->cqrySet[i]->shards_involved.size()==0){
            cout<<"No shards in list\n";
            fflush(stdout);
        } */


        // Append string representation of this txn.
        batchStr += msg->cqrySet[i]->getString();

        // Setting up data for BatchRequests Message.
        breq->copy_from_txn(txn_man, msg->cqrySet[i]);

        // Reset this txn manager.
        bool ready = txn_man->set_ready();
        assert(ready);
    }

    // Now we need to unset the txn_man again for the last txn of batch.
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

    if(!txn_man->is_2PC_Vote_recvd())
    {
        // Generating the hash representing the whole batch in last txn man.
        txn_man->set_hash(calculateHash(batchStr));
        txn_man->hashSize = txn_man->hash.length();
    }
    //For second local pbft
    else{
        // Generating the hash2 representing the whole batch in last txn man.
        txn_man->set_hash2(calculateHash(batchStr));
        txn_man->hashSize2 = txn_man->hash.length();
    }

    breq->copy_from_txn(txn_man);
    //Check 2PC info in batch
    /*if(msg->rtype == REQUEST_2PC){
        cout<<"In create_and_send_batchreq. Batch txn_id: "<<breq->txn_id
        <<" rc_txn_id: "<<breq->rc_txn_id<<" batch_id: "<<breq->batch_id
        <<" 2pcRequestRecvd: "<<breq->TwoPC_Request_recvd<<endl;
    }*/

    // Storing the BatchRequests message.
    txn_man->set_primarybatch(breq);


    // Storing all the signatures.
    vector<string> emptyvec;
    TxnManager *tman = get_transaction_manager(txn_man->get_txn_id() - 2, 0);
    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }

        // added for sharding
        if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
        // end
        breq->sign(i);
        tman->allsign.push_back(breq->signature); // Redundant
        emptyvec.push_back(breq->signature);
    }

    //printf("Enqueueing to msg queue BatchRequests: TID:%ld : RC_TID:%ld\n",breq->txn_id, breq->rc_txn_id);
    //fflush(stdout);

    // Send the BatchRequests message to all the other replicas in the same shard.
    vector<uint64_t> dest = nodes_to_send(g_node_id, g_node_id + g_shard_size);
    msg_queue.enqueue(get_thd_id(), breq, emptyvec, dest);
    emptyvec.clear();
}

//Function to send BatchRequest (Pre-Prepare) after receiving Vote_2PC or Global_Commit_2PC.
void WorkerThread::send_batchreq_2PC(ClientQueryBatch *msg, uint64_t tid){

    cout<<"In send_batchreq_2PC for tid: "<<tid<<endl;

    if(msg->rtype == VOTE_2PC){
        txn_man->set_2PC_Request_recvd();
        txn_man->set_2PC_Vote_recvd();
    }
    else if(msg->rtype == GLOBAL_COMMIT_2PC){
        txn_man->set_2PC_Vote_recvd();
        txn_man->set_2PC_Commit_recvd();
    }

    if(txn_man->is_2PC_Commit_recvd())cout<<"Updated! txn_id: "<<txn_man->get_txn_id()
    <<" rc_txn_id: "<<txn_man->get_txn_id_RC()<<endl;

    // Creating a new BatchRequests Message.
    Message *bmsg = Message::create_message(BATCH_REQ);
    BatchRequests *breq = (BatchRequests *)bmsg;
    /*breq->init(get_thd_id());

    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        char *bfr = (char *)malloc(msg->cqrySet[i]->get_size() + 1);
        msg->cqrySet[i]->copy_to_buf(bfr);
        Message *tmsg = Message::create_message(bfr);
        YCSBClientQueryMessage *yqry = (YCSBClientQueryMessage *)tmsg;
        free(bfr);

        // Setting up data for BatchRequests Message.
        breq->requestMsg[i] = yqry;
        breq->index.add(tid-get_batch_size()+1+i);
    }*/

    breq = txn_man->get_primarybatch();

    breq->copy_from_txn(txn_man);

    // Storing all the signatures.
    vector<string> emptyvec;
    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }

        // added for sharding
        if (!is_in_same_shard(i, g_node_id))
        {
            continue;
        }
        // end
        breq->sign(i);
        emptyvec.push_back(txn_man->batchreq->signature);
    }

    if(breq->TwoPC_Commit_recvd)printf("Enqueueing to msg queue BatchRequests from 2PC_Commit: TID:%ld : RC_TID:%ld\n",
    breq->txn_id, breq->rc_txn_id);
    fflush(stdout);

    // Send the BatchRequests message to all the other replicas in the same shard.
    vector<uint64_t> dest = nodes_to_send(g_node_id, g_node_id + g_shard_size);
    msg_queue.enqueue(get_thd_id(), breq, emptyvec, dest);
    emptyvec.clear();
    fflush(stdout);
}

/** Validates the contents of a message. */
bool WorkerThread::validate_msg(Message *msg)
{
    switch (msg->rtype)
    {
    case KEYEX:
        break;
    case CL_RSP:
        if (!((ClientResponseMessage *)msg)->validate())
        {
            assert(0);
        }
        break;
    case CL_BATCH:
        if (!((ClientQueryBatch *)msg)->validate())
        {
            assert(0);
        }
        break;
    case BATCH_REQ:
        if (!((BatchRequests *)msg)->validate(get_thd_id()))
        {
            assert(0);
        }
        break;
    case PBFT_CHKPT_MSG:
        if (!((CheckpointMessage *)msg)->validate())
        {
            assert(0);
        }
        break;
    case PBFT_PREP_MSG:
        if (!((PBFTPrepMessage *)msg)->validate())
        {
            assert(0);
        }
        break;
    case PBFT_COMMIT_MSG:
        if (!((PBFTCommitMessage *)msg)->validate())
        {
            assert(0);
        }
        break;

#if VIEW_CHANGES
    case VIEW_CHANGE:
        if (!((ViewChangeMsg *)msg)->validate(get_thd_id()))
        {
            assert(0);
        }
        break;
    case NEW_VIEW:
        if (!((NewViewMsg *)msg)->validate(get_thd_id()))
        {
            assert(0);
        }
        break;
#endif

    default:
        break;
    }

    return true;
}

/* Checks the hash and view of a message against current request. */
bool WorkerThread::checkMsg(Message *msg)
{
    if (msg->rtype == PBFT_PREP_MSG)
    {
        PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
        if ((txn_man->get_hash().compare(pmsg->hash) == 0) ||
            (get_current_view(get_thd_id()) == pmsg->view))
        {
            return true;
        }
    }
    else if (msg->rtype == PBFT_COMMIT_MSG)
    {
        PBFTCommitMessage *cmsg = (PBFTCommitMessage *)msg;
        if ((txn_man->get_hash().compare(cmsg->hash) == 0) ||
            (get_current_view(get_thd_id()) == cmsg->view))
        {
            return true;
        }
    }

    return false;
}

/**
 * Checks if the incoming PBFTPrepMessage can be accepted.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f messages are received it returns a true and 
 * sets the `is_prepared` flag for furtue identification.
 *
 * @param msg PBFTPrepMessage.
 * @return bool True if the transactions of this batch are prepared.
 */
bool WorkerThread::prepared(PBFTPrepMessage *msg)
{
    //cout << "Inside PREPARED: " << txn_man->get_txn_id() << " prep count: " << txn_man->get_prep_rsp_cnt()<<endl;
    //fflush(stdout);

    // Once prepared is set, no processing for further messages.
    if (txn_man->is_prepared())
    {
        return false;
    }

    // If BatchRequests messages has not arrived yet, then return false.
    if (txn_man->get_hash().empty())
    {
        cout<<"Batchrequest not received txn_id: "<<txn_man->get_txn_id()<<endl;
        // Store the message.
        txn_man->info_prepare.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            cout << txn_man->get_hash() << " :: " << msg->hash << "\n";
            cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
            fflush(stdout);
            return false;
        }
    }

    uint64_t prep_cnt = txn_man->decr_prep_rsp_cnt();
    if (prep_cnt == 0)
    {
        txn_man->set_prepared();
        return true;
    }

    return false;
}

/**
 * Checks if the incoming PBFTPrepMessage can be accepted in the second local pbft.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f messages are received it returns a true and 
 * sets the `is_prepared` flag for furtue identification.
 *
 * @param msg PBFTPrepMessage.
 * @return bool True if the transactions of this batch are prepared.
 */
bool WorkerThread::prepared2(PBFTPrepMessage *msg)
{
    cout << "Inside PREPARED2: " << txn_man->get_txn_id() << " prep count2: " << txn_man->get_prep_rsp_cnt2()<<endl;
    fflush(stdout);

    // Once prepared is set, no processing for further messages.
    if (txn_man->is_prepared2())
    {
        return false;
    }

    // If BatchRequests messages has not arrived yet, then return false.
    if (txn_man->get_hash().empty())
    {
        cout<<"Batchrequest2 not received txn_id: "<<txn_man->get_txn_id()<<endl;
        // Store the message.
        txn_man->info_prepare2.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            cout << txn_man->get_hash2() << " :: " << msg->hash << "\n";
            cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
            fflush(stdout);
            return false;
        }
    }

    uint64_t prep_cnt = txn_man->decr_prep_rsp_cnt2();
    if (prep_cnt == 0)
    {
        txn_man->set_prepared2();
        return true;
    }

    return false;
}

bool WorkerThread::check_2pc_request_recvd(Request_2PCBatch *msg){
    //cout << "Inside check_2pc_request_recvd for txn: " << msg->txn_id << "\n";
    //fflush(stdout);

    // Once 2PC_Request is set, no processing for further messages of same txn_id.
    if (txn_man->is_2PC_Request_recvd())
    {
        return false;
    }

    // Decrementing 2PC_Request_cnt
    uint64_t request_2pc_cnt = txn_man->decr_2PC_Request_cnt();
    //cout << "Request_2PC_Count: " << request_2pc_cnt << endl;
    //fflush(stdout);
    if (request_2pc_cnt == 0)
    {
        txn_man->set_2PC_Request_recvd();
        return true;
    }

    return false;
}

bool WorkerThread::check_2pc_vote_recvd(Vote_2PC *msg, TxnManager *txn_man){

    return true;
}

bool WorkerThread::check_2pc_global_commit_recvd(Global_Commit_2PC *msg, TxnManager *txn_man){

    return true;
}