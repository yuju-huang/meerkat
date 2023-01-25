// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/meerkatstore/meerkatir/client.cc:
 *   Meerkatir client interface (uses meerkatir for replcation and the
 *   meerkatstore transactional storage system).
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *           2018 Adriana Szekeres <aaasz@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "store/common/consts.h"
#include "store/meerkatstore/meerkatir/client.h"

#include <chrono>
#include <random>
#include <list>
#include <limits.h>
#include <thread>

#include <iostream>

// #define MEASURE 1

namespace meerkatstore {
namespace meerkatir {

using namespace std;

Client::Client(int nsthreads, int nShards, uint32_t id,
               std::shared_ptr<zip::client::client> client, zip::network::buffer&& buffer)
    : client_id(id), t_id(0),
      ziplogClient(client),
      ziplogBuffer(std::move(buffer))
{
    // Initialize all state here;
    srand(time(NULL));

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(1, ULLONG_MAX);

/*
    while (client_id == 0) {
        client_id = dis(gen);
    }
*/

    // Standard mersenne_twister_engine seeded with rd()
    // core_gen = std::mt19937(rd());

    Warning("Initializing Meerkatstore client with id [%lu]", client_id);

    Debug("Meerkatstore client [%lu] created!", client_id);
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void
Client::Begin()
{
    Debug("BEGIN [%lu]", t_id + 1);
    // Initialize data structures.
    txn = Transaction();
    t_id++;
}

/* Returns the value corresponding to the supplied key. */
int Client::Get(const string &key, string &value)
{
#ifdef MEASURE
    auto start = std::chrono::high_resolution_clock::now();
#endif
    Debug("GET [%lu : %s]", t_id, key.c_str());
    
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end()) {
        value = txn.getWriteSet().find(key)->second;
        return REPLY_OK;
    }

    // Send the GET operation.
    const auto leng = key.length();
    auto& req = ziplogBuffer.as<zip::api::zipkat_get>();
    req.message_type = zip::api::ZIPKAT_GET;
    req.client_id = ziplogClient->client_id();
    req.gsn = 0;
    req.data_length = leng;
    memcpy(req.key, key.data(), leng);
    Assert(req.length() < ziplogBuffer.length());

    static std::string empty;
    uint64_t timestamp;
    Assert(ziplogClient.get());
    const bool succ = ziplogClient->zipkat_get(ziplogBuffer, value, timestamp);
#ifdef MEASURE
    std::cout << "Get takes " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count() << std::endl;
#endif

    if (succ) {
        Debug("Adding [%s] with ts %lu", key.c_str(), timestamp);
        txn.addReadSet(key, timestamp);
        return REPLY_OK;
    } else {
        Debug("%s not found", key.c_str());
        return REPLY_FAIL;
    }
}

string Client::Get(const string &key)
{
    string value;
    Get(key, value);
    return value;
}

/* Sets the value corresponding to the supplied key. */
int Client::Put(const string &key, const string &value)
{
    Debug("PUT [%lu : %s]", t_id, key.c_str());
    // Update the write set.
    txn.addWriteSet(key, value);
    return REPLY_OK;
}

// This calls TryCommit, which will Commit if validation passes.
// TODO: make better method name.
int Client::Prepare()
{
    Debug("PREPARE [%lu] ", t_id);
    size_t txnLen = txn.serializedSize();
    auto& req = ziplogBuffer.as<zip::api::storage_insert_after>();
    req.message_type = zip::api::STORAGE_INSERT_AFTER;
    req.client_id = client_id;
    req.gsn_after = 0;
    req.num_slots = 1;
    auto commit_req = reinterpret_cast<zip::api::zipkat_commit_request*>(req.data);
    commit_req->data_length = txnLen;
    commit_req->nr_reads = txn.getReadSet().size();
    commit_req->nr_writes = txn.getWriteSet().size();
    txn.serialize((char*)commit_req->data);
    req.data_length = commit_req->length();

    Assert(req.length() < ziplogBuffer.length());
    Assert(ziplogClient.get());
    const auto result = ziplogClient->insert_after(ziplogBuffer);
    return result.second;
}

/* Attempts to commit the ongoing transaction. */
bool Client::Commit()
{
#ifdef MEASURE
    auto start = std::chrono::high_resolution_clock::now();
#endif
    int status = Prepare();

    if (status == REPLY_OK) {
        Debug("COMMIT [%lu]", t_id);
#ifdef MEASURE
        std::cout << "Commit succ takes " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count() << std::endl;
#endif
        return true;
    }

#ifdef MEASURE
    std::cout << "Commit abort takes " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count() << std::endl;
#endif
    Debug("ABORT [%lu]", t_id);
    return false;
}

/* Aborts the ongoing transaction. */
void
Client::Abort()
{
    Debug("ABORT [%lu]", t_id);
    // Do nothing.
}

/* Return statistics of most recent transaction. */
vector<int> Client::Stats()
{
    vector<int> v;
    return v;
}

} // namespace meerkatir
} // namespace meerkatstore
