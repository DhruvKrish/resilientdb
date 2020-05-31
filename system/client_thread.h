#ifndef _CLIENT_THREAD_H_
#define _CLIENT_THREAD_H_

#include "global.h"

#if VIEW_CHANGES == true || LOCAL_FAULT
#include "message.h"
#endif

class Workload;

class ClientThread : public Thread
{
public:
    RC run();

#if VIEW_CHANGES == true
    void resend_msg(ClientQueryBatch *symsg);
#endif

    void setup();
    void send_key();

private:
    uint64_t last_send_time;
    uint64_t send_interval;
#if AHL
    // Count of number of batches sent by client.
    uint64_t txn_batch_sent_cnt;
#endif
#if AHLRandom
    // Variable to count the number of shards involved in the cross shard txn other than reference committee
    uint64_t shardRandom;
#endif
};

#endif
