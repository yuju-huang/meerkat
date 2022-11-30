// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/replica.cc:
 *   IR Replica server
 *
 **********************************************************************/

#include "replication/common/consts.h"
#include "replication/common/ziplog_message.h"
#include "replication/meerkatir/replica.h"

#include <csignal>
#include <cstdint>

#include <set>

#include <unordered_map>

namespace replication {
namespace meerkatir {

using namespace std;

/** signal to stop the storage server */
static std::atomic<bool> stop = false;

Replica::Replica(transport::Configuration config, int myIdx,
                     Transport *transport, AppReplica *app, uint8_t thread_id)
    : config(std::move(config)), myIdx(myIdx), transport(transport), app(app),
      ziplogManager(zip::consts::rdma::DEFAULT_DEVICE, zip::consts::rdma::DEAULT_PORT)
{
    if (transport != NULL) {
        transport->Register(this, myIdx);
    } else {
        // we use this for micorbenchmarking, but still issue a warning
        Warning("NULL transport provided to the replication layer");
    }

    const int id = 100 + thread_id;
    const int ssn = 0; //zip::api::subscriber_intro::UNSPECIFIED_START;
    const bool seq = true;
    ziplogSubscriber = std::make_shared<zip::subscriber::subscriber>(
        ziplogManager, kOrderAddr, id, ssn, seq,
        [](zip::api::subscriber_log_entry& entry, void* ctx) {
            Debug("Receive entry.gsn=%ld, data_length=%ld, client_id=%ld\n",
                entry.gsn, entry.data_length, entry.client_id);
            auto replica = reinterpret_cast<Replica*>(ctx);
            replica->TryCommit(entry.gsn, reinterpret_cast<char*>(entry.data));
        },
        this);

    // Install handler to finish the threads
    std::signal(SIGINT, [] (int signal) { stop.store(true, std::memory_order_relaxed); });

    // Start accept thread
    threads.emplace_back(&Replica::AcceptThread, this);
    threads.emplace_back(&Replica::PollSubscriber, this);
}

Replica::~Replica() {
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : threads)
        t.join();
}

void Replica::PollSubscriber() {
    while (!stop.load(std::memory_order_relaxed)) {
        ziplogSubscriber->poll();
    }
}

// Accept connection from the clients. This connections are for responding the Commit results.
void Replica::AcceptThread() {
    auto manager = zip::network::manager(
        zip::consts::rdma::DEFAULT_DEVICE, zip::consts::rdma::DEAULT_PORT);
    manager.bind_server(kServerPort);
    zip::api::client_intro intro;

    while (!stop.load(std::memory_order_relaxed)) {
        // try to accept a connection and exchange the first message
        auto result = manager.accept(&intro, intro.length());
        if (result) {
            auto& [send_queue, buffer, length, _] = *result;

            // verify the received message and add the client or subscriber
            auto req_type = *static_cast<uint64_t*>(buffer);
            switch (req_type) {
                case zip::api::CLIENT_INTRO:
                {
                    // add the client to the state
                    auto& intro = *static_cast<zip::api::client_intro*>(buffer);
                    Assert(length == intro.length());

                    std::unique_lock wl(sendQueueLock);
                    Assert(sendQueues.find(intro.client_id) == sendQueues.end());
                    sendQueues[intro.client_id] = std::move(send_queue);
                    Debug("Replica connect to client-%ld (%d,%d)", intro.client_id,
                        sendQueues[intro.client_id].send_queue_qpn(),
                        sendQueues[intro.client_id].recv_queue_qpn());

                    // TODO: remove, this works I don't know why.. without this the send at TryCommit get IBV_WC_RETRY_EXC_ERR
/*
                    commit_response ack;
                    ack.message_type = COMMIT_RESPONSE;
                    ack.txn_nr = 8888;
                    auto& q = sendQueues[intro.client_id];
                    q.send(&ack, ack.length());
                    Debug("Send magic 8888 to (%d,%d) done\n",
                        q.send_queue_qpn(), q.recv_queue_qpn());
*/
                }

                break;
                default:
                    Assert(false);
                    Warning("Received an unknown type of message (%lu)", req_type);
            }
        }
    }
}

// Map from txn_nr to respBuf.
/*
struct PairHash {
    template <class T1, class T2>
    std::size_t operator () (const std::pair<T1,T2> &p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);

        // Mainly for demonstration purposes, i.e. works but is overly simple
        // In the real world, use sth. like boost.hash_combine
        return h1 ^ h2;  
    }
};
*/

// Map from client_id to (map from txn_nr to respBuf).
static std::unordered_map<uint64_t, std::unordered_map<uint64_t, char*>> commitRequests{};
//static std::unordered_map<std::pair<uint64_t, uint64_t>, char*, PairHash> commitRequests{};
static atomic<bool> gReqDone = false;
void Replica::TryCommit(uint64_t gsn, char *reqBuf) {
    Assert(reqBuf);
    auto *req = reinterpret_cast<consensus_request_header_t *>(reqBuf);
    const auto client_id = req->client_id;
    const auto txn_nr = req->txn_nr;
    Debug("[client-%lu - txn_nr-%lu] Received consensus op number req_nr-%lu:\n",
          client_id, txn_nr, req->req_nr);

    while (commitRequests.find(client_id) == commitRequests.end());
    auto map = commitRequests[client_id];
    while (map.find(txn_nr) == map.end());
    char* respBuf = map.at(txn_nr);
    size_t respLen;

    txnid_t txnid = make_pair(client_id, txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        //if (clientreq_nr <= entry->req_nr) {
            // If a client request number from the past, ignore it
        //    return;
        //}
        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // It's the first prepare request we've seen for this transaction,
        // save it as tentative, in initial view, with initial status
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_TENTATIVE, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction status;
    // the app will decide whether to execute this prepare request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    // string result;
    app->ExecConsensusUpcall(txnid, entry, req->nr_reads,
                             req->nr_writes, gsn, req->id,
                             reqBuf + sizeof(consensus_request_header_t),
                             respBuf, respLen);

    if (entry->txn_status != crt_txn_status) {
        // Update record
        //record.SetResult(txnid, result);
        record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
        //crt_txn_result = result;
        crt_txn_state = RECORD_STATE_TENTATIVE;
    }

#if 1
    // fill in the replication layer specific fields
    // TODO: make this more general
    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);
    resp->view = crt_txn_view;
    resp->replicaid = myIdx;
    resp->req_nr = req->req_nr;
    resp->txn_nr = txn_nr;
    resp->finalized = (crt_txn_state == RECORD_STATE_FINALIZED);
    Debug("Send consensus_response_t client_id=%ld, req_nr=%ld, txn_nr=%ld, status=%d\n",
        client_id, resp->req_nr, resp->txn_nr, resp->status);
    gReqDone.store(true, std::memory_order_relaxed);
}
#else
void Replica::TryCommit(uint64_t gsn, char *reqBuf) {
    auto *req = reinterpret_cast<consensus_request_header_t *>(reqBuf);
    consensus_response_t resp;
    size_t respLen;

    Debug("[client-%lu - txn_nr-%lu] Received consensus op number req_nr-%lu:\n",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        //if (clientreq_nr <= entry->req_nr) {
            // If a client request number from the past, ignore it
        //    return;
        //}
        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // It's the first prepare request we've seen for this transaction,
        // save it as tentative, in initial view, with initial status
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_TENTATIVE, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction status;
    // the app will decide whether to execute this prepare request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    // string result;
    app->ExecConsensusUpcall(txnid, entry, req->nr_reads,
                             req->nr_writes, gsn, req->id,
                             reqBuf + sizeof(consensus_request_header_t),
                             reinterpret_cast<char*>(&resp), respLen);

    if (entry->txn_status != crt_txn_status) {
        // Update record
        //record.SetResult(txnid, result);
        record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
        //crt_txn_result = result;
        crt_txn_state = RECORD_STATE_TENTATIVE;
    }

    commit_response ack;
    ack.message_type = COMMIT_RESPONSE;
    // TODO: deduplicate
    ack.req_nr = resp.req_nr;
    ack.txn_nr = resp.txn_nr;
    ack.replicaid = resp.replicaid;
    ack.view = resp.view;
    ack.status = resp.status;

    std::shared_lock rl(sendQueueLock);
    Debug("Send ack client_id=%ld, req_nr=%ld, txn_nr=%ld, status=%d to (%d,%d)\n",
        req->client_id, ack.req_nr, ack.txn_nr, ack.status,
        sendQueues[req->client_id].send_queue_qpn(),
        sendQueues[req->client_id].recv_queue_qpn());
    sendQueues.at(req->client_id).send(&ack, ack.length());
    Debug("Send ack client_id=%ld, req_nr=%ld, txn_nr=%ld, status=%d to (%d,%d) done\n",
        req->client_id, ack.req_nr, ack.txn_nr, ack.status,
        sendQueues[req->client_id].send_queue_qpn(),
        sendQueues[req->client_id].recv_queue_qpn());
}
#endif

void Replica::ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) {
    Debug("Replica::ReceiveRequest, reqType=%d\n", reqType);
    size_t respLen;
    switch(reqType) {
        case unloggedReqType:
            HandleUnloggedRequest(reqBuf, respBuf, respLen);
            break;
        case inconsistentReqType:
            HandleInconsistentRequest(reqBuf, respBuf, respLen);
            break;
        case consensusReqType: {
#if 1
            auto *req = reinterpret_cast<consensus_request_header_t *>(reqBuf);
            Debug("[%lu - %lu] Received consensus op number %lu:\n",
              req->client_id, req->txn_nr, req->req_nr);

            // TODO: add client_id
            const auto txn_nr = req->txn_nr;
            auto map = commitRequests[req->client_id];
            Assert(map.find(txn_nr) == map.end());
            map[txn_nr] = respBuf;
            Debug("HandleConsensusRequest, set commitRequests[(%ld,%ld)]=%p\n", req->client_id, txn_nr, respBuf);
            while(!gReqDone.load(std::memory_order_acquire));
            respLen = sizeof(consensus_response_t);
            map.erase(txn_nr);
            gReqDone.store(false, std::memory_order_relaxed);
            Debug("HandleConsensusRequest, (%ld,%ld) done, sent response\n", req->client_id, txn_nr, respBuf);
#else
            HandleConsensusRequest(reqBuf, respBuf, respLen);
#endif
            break;
        }
        case finalizeConsensusReqType:
            HandleFinalizeConsensusRequest(reqBuf, respBuf, respLen);
            break;
        default:
            Warning("Unrecognized rquest type: %d", reqType);
    }

    // For every request, we need to send a response (because we use eRPC)
    if (!(transport->SendResponse(respLen)))
        Warning("Failed to send reply message");
}

void Replica::HandleUnloggedRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    // ignore requests from the past
    app->UnloggedUpcall(reqBuf, respBuf, respLen);
}

void Replica::HandleInconsistentRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    // Shouldn't get into here if using ziplog.
    Assert(false);

    auto *req = reinterpret_cast<inconsistent_request_t *>(reqBuf);

    Debug("[%lu - %lu] Received inconsistent op nr %lu\n",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        if (req->req_nr <= entry->req_nr) {
            Warning("Client request from the past.");
            // If a client request number from the past, ignore it
            return;
        }

        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // We've never seen this transaction before,
        // save it as tentative, in initial view,
        // with initial status
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_FINALIZED, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction state;
    // the app will decide whether to execute this commit/abort request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    app->ExecInconsistentUpcall(txnid, entry, req->commit);

    // TODO: we use eRPC and it expects replies in order so we need
    // to send replies to all requests
    auto *resp = reinterpret_cast<inconsistent_response_t *>(respBuf);
    resp->req_nr = req->req_nr;
    respLen = sizeof(inconsistent_response_t);

    // TODO: for now just trim the log as soon as the transaction was finalized
    // this is not safe for a complete checkpoint
    record.Remove(txnid);
}

void Replica::HandleConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    auto *req = reinterpret_cast<consensus_request_header_t *>(reqBuf);
    Debug("[%lu - %lu] Received consensus op number %lu:\n",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        //if (clientreq_nr <= entry->req_nr) {
            // If a client request number from the past, ignore it
        //    return;
        //}
        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // It's the first prepare request we've seen for this transaction,
        // save it as tentative, in initial view, with initial status
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_TENTATIVE, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction status;
    // the app will decide whether to execute this prepare request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    // string result;
    app->ExecConsensusUpcall(txnid, entry, req->nr_reads,
                             req->nr_writes, req->timestamp, req->id,
                             reqBuf + sizeof(consensus_request_header_t),
                             respBuf, respLen);

    if (entry->txn_status != crt_txn_status) {
        // Update record
        //record.SetResult(txnid, result);
        record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
        //crt_txn_result = result;
        crt_txn_state = RECORD_STATE_TENTATIVE;
    }

    // fill in the replication layer specific fields
    // TODO: make this more general
    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);
    resp->view = crt_txn_view;
    resp->replicaid = myIdx;
    resp->req_nr = req->req_nr;
    resp->finalized = (crt_txn_state == RECORD_STATE_FINALIZED);

    respLen = sizeof(consensus_response_t);
}

void Replica::HandleFinalizeConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    auto *req = reinterpret_cast<finalize_consensus_request_t *>(reqBuf);

    Debug("[%lu - %lu] Received finalize consensus for req %lu",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record for the request
    RecordEntry *entry = record.Find(txnid);
    if (entry != NULL) {
        //if (clientreq_nr < entry->req_nr) {
            // If finalize for a different operation number, then ignore
            // TODO: what if we missed a prepare phase, thus clientreq_nr > entry->req_nr?
        //    return;
        //}

        // TODO: check the view? If request in lower
        // view just reply with the new view and new state

        // Mark entry as finalized
        record.SetStatus(txnid, RECORD_STATE_FINALIZED);

        // if (msg.status() != entry->result) {
        //     // Update the result
        //     // TODO: set the timestamp and status of the transaction
        //     entry->result = msg.result();
        // }

        // Send the reply
        auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
        resp->view = entry->view;
        resp->replicaid = myIdx;
        resp->req_nr = req->req_nr;
        respLen = sizeof(finalize_consensus_response_t);

        Debug("[%lu - %lu] Operation found and consensus finalized", req->client_id, req->txn_nr);
    } else {
        // Ignore?
        // Send the reply
        auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
        resp->view = 0;
        resp->replicaid = myIdx;
        resp->req_nr = req->req_nr;
        respLen = sizeof(finalize_consensus_response_t);
    }
}

void Replica::PrintStats() {
}

} // namespace ir
} // namespace replication
