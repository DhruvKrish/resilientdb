#include "helper.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "array.h"

void TxnStats::init()
{
    starttime = 0;
    wait_starttime = get_sys_clock();
    total_process_time = 0;
    process_time = 0;
    total_local_wait_time = 0;
    local_wait_time = 0;
    total_remote_wait_time = 0;
    remote_wait_time = 0;
    write_cnt = 0;
    abort_cnt = 0;

    total_work_queue_time = 0;
    work_queue_time = 0;
    total_work_queue_cnt = 0;
    work_queue_cnt = 0;
    total_msg_queue_time = 0;
    msg_queue_time = 0;
    total_abort_time = 0;
    time_start_pre_prepare = 0;
    time_start_prepare = 0;
    time_start_commit = 0;
    time_start_execute = 0;

    clear_short();
}

void TxnStats::clear_short()
{

    work_queue_time_short = 0;
    cc_block_time_short = 0;
    cc_time_short = 0;
    msg_queue_time_short = 0;
    process_time_short = 0;
    network_time_short = 0;
}

void TxnStats::reset()
{
    wait_starttime = get_sys_clock();
    total_process_time += process_time;
    process_time = 0;
    total_local_wait_time += local_wait_time;
    local_wait_time = 0;
    total_remote_wait_time += remote_wait_time;
    remote_wait_time = 0;
    write_cnt = 0;

    total_work_queue_time += work_queue_time;
    work_queue_time = 0;
    total_work_queue_cnt += work_queue_cnt;
    work_queue_cnt = 0;
    total_msg_queue_time += msg_queue_time;
    msg_queue_time = 0;

    clear_short();
}

void TxnStats::abort_stats(uint64_t thd_id)
{
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_work_queue_cnt += work_queue_cnt;
    assert(total_process_time >= process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long,
                            uint64_t timespan_short)
{
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_work_queue_cnt += work_queue_cnt;
    assert(total_process_time >= process_time);

    if (IS_LOCAL(txn_id))
    {
        PRINT_LATENCY("lat_s %ld %ld %f %f %f %f\n", txn_id, work_queue_cnt, (double)timespan_short / BILLION, (double)work_queue_time / BILLION, (double)msg_queue_time / BILLION, (double)process_time / BILLION);
    }
    else
    {
        PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f\n", txn_id, work_queue_cnt, (double)timespan_short / BILLION, (double)total_work_queue_time / BILLION, (double)total_msg_queue_time / BILLION, (double)total_process_time / BILLION);
    }

    if (!IS_LOCAL(txn_id))
    {
        return;
    }
}

void Transaction::init()
{
    txn_id = UINT64_MAX;
    batch_id = UINT64_MAX;
    txn_id_RC = UINT64_MAX;
    cross_shard_txn=false;

    reset(0);
}

void Transaction::reset(uint64_t pool_id)
{
    rc = RCOK;
}

void Transaction::release(uint64_t pool_id)
{
    DEBUG("Transaction release\n");
}

void TxnManager::init(uint64_t pool_id, Workload *h_wl)
{
    if (!txn)
    {
        DEBUG_M("Transaction alloc\n");
        txn_pool.get(pool_id, txn);
    }

    if (!query)
    {
        DEBUG_M("TxnManager::init Query alloc\n");
        qry_pool.get(pool_id, query);
    }

    sem_init(&rsp_mutex, 0, 1);
    return_id = UINT64_MAX;

    this->h_wl = h_wl;

    txn_ready = true;

    prepared = false;
    prepared2 = false;
    committed_local = false;
    committed_local2 = false;
    prep_rsp_cnt = 2 * g_min_invalid_nodes;
    commit_rsp_cnt = prep_rsp_cnt+1;
    prep_rsp_cnt2 = 2 * g_min_invalid_nodes;
    commit_rsp_cnt2 = prep_rsp_cnt2+1;
    chkpt_cnt = 2 * g_min_invalid_nodes;

    //Counters of 2PC messages
    TwoPC_Request_cnt=g_min_invalid_nodes+1;
    TwoPC_Vote_cnt=g_min_invalid_nodes+1;
    TwoPC_Commit_cnt=g_min_invalid_nodes+1;
    //2PC messages received should be false initially
    TwoPC_Request_recvd=false;
    TwoPC_Vote_recvd=false;
    TwoPC_Commit_recvd=false;

    batchreq = NULL;

    txn_stats.init();
}

// reset after abort
void TxnManager::reset()
{
    rsp_cnt = 0;
    aborted = false;
    return_id = UINT64_MAX;
    //twopl_wait_start = 0;

    assert(txn);
    assert(query);
    txn->reset(get_thd_id());

    // Stats
    txn_stats.reset();
}

void TxnManager::release(uint64_t pool_id)
{

    uint64_t tid = get_txn_id();

    qry_pool.put(pool_id, query);
    query = NULL;
    txn_pool.put(pool_id, txn);
    //Release shard list array if transaction was cross sharded
    if(get_cross_shard_txn())txn->shards_involved.release();
    txn = NULL;

    txn_ready = true;

    hash.clear();
    hash2.clear();
    prepared = false;
    prepared2 = false;

    prep_rsp_cnt = 2 * g_min_invalid_nodes;
    commit_rsp_cnt = prep_rsp_cnt+1;
    prep_rsp_cnt2 = 2 * g_min_invalid_nodes;
    commit_rsp_cnt2 = prep_rsp_cnt2+1;
    chkpt_cnt = 2 * g_min_invalid_nodes + 1;
    //Counters of 2PC messages
    TwoPC_Request_cnt=g_min_invalid_nodes+1;
    TwoPC_Vote_cnt=g_min_invalid_nodes+1;
    TwoPC_Commit_cnt=g_min_invalid_nodes+1;
    //2PC messages received should be false initially
    TwoPC_Request_recvd=false;
    TwoPC_Vote_recvd=false;
    TwoPC_Commit_recvd=false;

    release_all_messages(tid);

    txn_stats.init();
}

void TxnManager::reset_query()
{
    ((YCSBQuery *)query)->reset();
}

RC TxnManager::commit()
{
    DEBUG("Commit %ld\n", get_txn_id());

    commit_stats();
    return Commit;
}

RC TxnManager::start_commit()
{
    RC rc = RCOK;
    DEBUG("%ld start_commit RO?\n", get_txn_id());
    return rc;
}

int TxnManager::received_response(RC rc)
{
    assert(txn->rc == RCOK);
    if (txn->rc == RCOK)
        txn->rc = rc;

    --rsp_cnt;

    return rsp_cnt;
}

bool TxnManager::waiting_for_response()
{
    return rsp_cnt > 0;
}

void TxnManager::commit_stats()
{
    uint64_t commit_time = get_sys_clock();
    uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
    uint64_t timespan_long = commit_time - txn_stats.starttime;
    INC_STATS(get_thd_id(), total_txn_commit_cnt, 1);

    if (!IS_LOCAL(get_txn_id()))
    {
        txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long, timespan_short);
        return;
    }

    INC_STATS(get_thd_id(), txn_cnt, 1);
    INC_STATS(get_thd_id(), txn_run_time, timespan_long);
    INC_STATS(get_thd_id(), single_part_txn_cnt, 1);

    txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long, timespan_short);
}

void TxnManager::register_thread(Thread *h_thd)
{
    this->h_thd = h_thd;
}

void TxnManager::set_txn_id(txnid_t txn_id)
{
    txn->txn_id = txn_id;
}

txnid_t TxnManager::get_txn_id()
{
    return txn->txn_id;
}

void TxnManager::set_txn_id_RC(txnid_t txn_id_RC)
{
    txn->txn_id_RC = txn_id_RC;
    txn->batch_id = txn_id_RC;
}

txnid_t TxnManager::get_txn_id_RC()
{
    return txn->txn_id_RC;
}

//set cross_shard_txn flag
void TxnManager::set_cross_shard_txn(){
    txn->cross_shard_txn=true;
}

//check if cross_shard_txn flag is set
bool TxnManager::get_cross_shard_txn(){
    return txn->cross_shard_txn;
}

//Initialize list of shards involved in the transaction with a capacity
void TxnManager::init_shards_involved(uint64_t capacity){
    txn->shards_involved.init(capacity);
}

//Add a shard to the list of shards involved in the transaction
void TxnManager::set_shards_involved(uint64_t shard_number){
    txn->shards_involved.add(shard_number);
}

Array<uint64_t> TxnManager::get_shards_involved(){
    return txn->shards_involved;
}

Workload *TxnManager::get_wl()
{
    return h_wl;
}

uint64_t TxnManager::get_thd_id()
{
    if (h_thd)
        return h_thd->get_thd_id();
    else
        return 0;
}

BaseQuery *TxnManager::get_query()
{
    return query;
}

void TxnManager::set_query(BaseQuery *qry)
{
    query = qry;
}

uint64_t TxnManager::incr_rsp(int i)
{
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = ++this->rsp_cnt;
    sem_post(&rsp_mutex);
    return result;
}

uint64_t TxnManager::decr_rsp(int i)
{
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = --this->rsp_cnt;
    sem_post(&rsp_mutex);
    return result;
}

RC TxnManager::validate()
{
    return RCOK;
}

/* Generic Helper functions. */

string TxnManager::get_hash()
{
    return hash;
}

string TxnManager::get_hash2()
{
    return hash2;
}

void TxnManager::set_hash(string hsh)
{
    hash = hsh;
    hashSize = hash.length();
}

void TxnManager::set_hash2(string hsh)
{
    hash2 = hsh;
    hashSize2 = hash2.length();
}

uint64_t TxnManager::get_hashSize()
{
    return hashSize;
}

uint64_t TxnManager::get_hashSize2()
{
    return hashSize2;
}


void TxnManager::set_primarybatch(BatchRequests *breq) 
{
	char *buf = create_msg_buffer(breq);
	Message *deepMsg = deep_copy_msg(buf, breq);
	batchreq = (BatchRequests *)deepMsg;
	delete_msg_buffer(buf);
}	

BatchRequests* TxnManager::get_primarybatch()
{
    BatchRequests * brequest;
	char *buf = create_msg_buffer(this->batchreq);
	Message *deepMsg = deep_copy_msg(buf, this->batchreq);
	brequest = (BatchRequests *)deepMsg;
	delete_msg_buffer(buf);

    return brequest;
}

bool TxnManager::is_chkpt_ready()
{
    return chkpt_flag;
}

void TxnManager::set_chkpt_ready()
{
    chkpt_flag = true;
}

uint64_t TxnManager::decr_chkpt_cnt()
{
    chkpt_cnt--;
    return chkpt_cnt;
}

uint64_t TxnManager::get_chkpt_cnt()
{
    return chkpt_cnt;
}

/* Helper functions for PBFT. */
void TxnManager::set_prepared()
{
    prepared = true;
}

bool TxnManager::is_prepared()
{
    return prepared;
}

uint64_t TxnManager::decr_prep_rsp_cnt()
{
    prep_rsp_cnt--;
    return prep_rsp_cnt;
}

uint64_t TxnManager::get_prep_rsp_cnt()
{
    return prep_rsp_cnt;
}

void TxnManager::set_prepared2()
{
    prepared2 = true;
}

bool TxnManager::is_prepared2()
{
    return prepared2;
}

uint64_t TxnManager::decr_prep_rsp_cnt2()
{
    prep_rsp_cnt2--;
    return prep_rsp_cnt2;
}

uint64_t TxnManager::get_prep_rsp_cnt2()
{
    return prep_rsp_cnt2;
}

/************************************/

/* Helper functions for PBFT. */

void TxnManager::set_committed()
{
    committed_local = true;
}

bool TxnManager::is_committed()
{
    return committed_local;
}


void TxnManager::add_commit_msg(PBFTCommitMessage *pcmsg) 
{
	char *buf = create_msg_buffer(pcmsg);
	Message *deepMsg = deep_copy_msg(buf, pcmsg);
	commit_msgs.push_back((PBFTCommitMessage *)deepMsg);
	delete_msg_buffer(buf);
}

uint64_t TxnManager::decr_commit_rsp_cnt()
{
    commit_rsp_cnt--;
    return commit_rsp_cnt;
}

uint64_t TxnManager::get_commit_rsp_cnt()
{
    return commit_rsp_cnt;
}


void TxnManager::set_committed2()
{
    committed_local2 = true;
}

bool TxnManager::is_committed2()
{
    return committed_local2;
}


/*void TxnManager::add_commit_msg2(PBFTCommitMessage *pcmsg)
{
	char *buf = create_msg_buffer(pcmsg);
	Message *deepMsg = deep_copy_msg(buf, pcmsg);
	commit_msgs.push_back((PBFTCommitMessage *)deepMsg);
	delete_msg_buffer(buf);
}*/

uint64_t TxnManager::decr_commit_rsp_cnt2()
{
    commit_rsp_cnt2--;
    return commit_rsp_cnt2;
}

uint64_t TxnManager::get_commit_rsp_cnt2()
{
    return commit_rsp_cnt2;
}

/*****************************/
/*Helper functions for 2PC Message Processing*/

//2PC Request Messages
void TxnManager::set_2PC_Request_recvd()
{
    TwoPC_Request_recvd = true;
}

bool TxnManager::is_2PC_Request_recvd()
{
    return TwoPC_Request_recvd;
}

uint64_t TxnManager::decr_2PC_Request_cnt()
{
    TwoPC_Request_cnt--;
    return TwoPC_Request_cnt;
}

uint64_t TxnManager::get_2PC_Request_cnt()
{
    return TwoPC_Request_cnt;
}

//2PC Vote Messages
void TxnManager::set_2PC_Vote_recvd()
{
    TwoPC_Vote_recvd = true;
}

bool TxnManager::is_2PC_Vote_recvd()
{
    return TwoPC_Vote_recvd;
}

uint64_t TxnManager::decr_2PC_Vote_cnt()
{
    TwoPC_Vote_cnt--;
    return TwoPC_Vote_cnt;
}

uint64_t TxnManager::get_2PC_Vote_cnt()
{
    return TwoPC_Vote_cnt;
}

//2PC Commit Messages
void TxnManager::set_2PC_Commit_recvd()
{
    TwoPC_Commit_recvd = true;
}

bool TxnManager::is_2PC_Commit_recvd()
{
    return TwoPC_Commit_recvd;
}

uint64_t TxnManager::decr_2PC_Commit_cnt()
{
    TwoPC_Commit_cnt--;
    return TwoPC_Commit_cnt;
}

uint64_t TxnManager::get_2PC_Commit_cnt()
{
    return TwoPC_Commit_cnt;
}

/*****************************/

//broadcasts prepare message to all nodes
void TxnManager::send_pbft_prep_msgs()
{
    if(is_2PC_Vote_recvd()){
        // printf("Send PBFT_PREP_MSG message txn_id: %ld to %d nodes\n", get_txn_id(), g_shard_size - 1);
        fflush(stdout);
    }

    Message *msg = Message::create_message(this, PBFT_PREP_MSG);
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;

    //Assign which local pbft the message is for
    if(!is_2PC_Vote_recvd() && !is_2PC_Commit_recvd()) pmsg->first_local_pbft = true;
    else pmsg->first_local_pbft = false;

#if LOCAL_FAULT == true || VIEW_CHANGES
    if (get_prep_rsp_cnt() > 0)
    {
        decr_prep_rsp_cnt();
    }
#endif

    vector<string> emptyvec;
    vector<uint64_t> dest;
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
        dest.push_back(i);
    }

    msg_queue.enqueue(get_thd_id(), pmsg, emptyvec, dest);
    dest.clear();
}

//broadcasts commit message to all nodes
void TxnManager::send_pbft_commit_msgs()
{
    // cout << "Send PBFT_COMMIT_MSG messages " << get_txn_id() <<" rc_txn_id "<< get_txn_id_RC() <<"\n";

    Message *msg = Message::create_message(this, PBFT_COMMIT_MSG);
    PBFTCommitMessage *cmsg = (PBFTCommitMessage *)msg;

    //Assign which local pbft the message is for
    if(!is_2PC_Vote_recvd() && !is_2PC_Commit_recvd()) cmsg->first_local_pbft = true;
    else cmsg->first_local_pbft = false;

#if LOCAL_FAULT == true || VIEW_CHANGES
    if (get_commit_rsp_cnt() > 0)
    {
        decr_commit_rsp_cnt();
    }
#endif

    vector<string> emptyvec;
    vector<uint64_t> dest;
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
        dest.push_back(i);
    }

    msg_queue.enqueue(get_thd_id(), cmsg, emptyvec, dest);
    dest.clear();
}

#if !TESTING_ON

void TxnManager::release_all_messages(uint64_t txn_id)
{
    if ((txn_id + 3) % get_batch_size() == 0)
    {
        allsign.clear();
    }
    else if ((txn_id + 1) % get_batch_size() == 0)
    {
        info_prepare.clear();
        info_commit.clear();
	
	Message::release_message(batchreq);

	PBFTCommitMessage *cmsg;
	while(commit_msgs.size()>0)
	{
		cmsg = (PBFTCommitMessage *)this->commit_msgs[0];
		commit_msgs.erase(commit_msgs.begin());
		Message::release_message(cmsg);
	}
    }
}

#endif // !TESTING

//broadcasts checkpoint message to all nodes
void TxnManager::send_checkpoint_msgs()
{
    DEBUG("%ld Send PBFT_CHKPT_MSG message to %d\n nodes", get_txn_id(), g_node_cnt - 1);

    Message *msg = Message::create_message(this, PBFT_CHKPT_MSG);
    CheckpointMessage *ckmsg = (CheckpointMessage *)msg;

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

    msg_queue.enqueue(get_thd_id(), ckmsg, emptyvec, dest);
    dest.clear();
}
