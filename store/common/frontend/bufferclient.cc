// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/**********************************************************************
 * store/common/frontend/bufferclient.cc:
 *   Single shard buffering client implementation.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#include "store/common/frontend/bufferclient.h"
#include "store/common/consts.h"
#include "util/consts.h"

using namespace std;

BufferClient::BufferClient(uint32_t id)
    : client_id(id), txn(),
      ziplogManager(zip::consts::rdma::DEFAULT_DEVICE, zip::consts::rdma::DEAULT_PORT),
      ziplogBuffer(ziplogManager.get_buffer(zip::consts::PAGE_SIZE))
{
    // TODO: Initialize ziplog
    static constexpr int kNumCpus = 32;
    static constexpr int kNumCpusPerNuma = kNumCpus / 2;
    const int cpu_id = 2 * (id % kNumCpusPerNuma) + 1;
    Assert(cpu_id < kNumCpus);
    ziplogClient = std::make_shared<zip::client::client>(
        ziplogManager, kOrderAddr, id, kZiplogShardId, cpu_id, kZiplogClientRate);
}

BufferClient::~BufferClient() { }

/* Begins a transaction. */
void
BufferClient::Begin(uint64_t tid)
{
    // Initialize data structures.
    txn = Transaction();
    this->tid = tid;
}

/* Get value for a key.
 * Returns 0 on success, else -1. */
void
BufferClient::Get(const string &key, Promise *promise)
{
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end()) {
        promise->Reply(REPLY_OK, (txn.getWriteSet().find(key))->second);
        return;
    }

    // TODO: we do not support multi-versioning;
    // for now, just always take the latest value from server,
    // it will abort if not consistent anyways (key was written in the meantime)
    // Consistent reads, check the read set.
//    if (txn.getReadSet().find(key) != txn.getReadSet().end()) {
//        // read from the server at same timestamp.
//        txnclient->Get(tid, key, (txn.getReadSet().find(key))->second, promise);
//        return;
//    }
    const auto leng = key.length();
    auto& req = ziplogBuffer.as<zip::api::zipkat_get>();
    req.message_type = zip::api::ZIPKAT_GET;
    req.client_id = client_id;
    req.gsn = 0;
    req.data_length = leng;
    memcpy(req.key, key.data(), leng);
    Assert(req.length() < ziplogBuffer.length());

    static std::string empty;
    std::string value;
    uint64_t timestamp;
    Assert(ziplogClient.get());
    if (ziplogClient->zipkat_get(ziplogBuffer, value, timestamp)) {
        Debug("Adding [%s] with ts %lu", key.c_str(), timestamp);
        txn.addReadSet(key, timestamp);
    } else {
        Debug("%s not found", key.c_str());
        promise->Reply(REPLY_FAIL);
    }
}

/* Set value for a key. (Always succeeds).
 * Returns 0 on success, else -1. */
void
BufferClient::Put(const string &key, const string &value, Promise *promise)
{
    // Update the write set.
    txn.addWriteSet(key, value);
    promise->Reply(REPLY_OK);
}

/* Prepare the transaction. */
void
BufferClient::Prepare(Promise *promise)
{
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
    promise->Reply(result.second);
}

void
BufferClient::Commit(const Timestamp &timestamp, Promise *promise)
{
    Assert(false);
}

/* Aborts the ongoing transaction. */
void
BufferClient::Abort(Promise *promise)
{
    // Do nothing.
}

void
BufferClient::PrepareWrite(Promise *promise)
{
    Assert(false);
}

void
BufferClient::PrepareRead(const Timestamp& timestamp, Promise *promise)
{
    Assert(false);
}
