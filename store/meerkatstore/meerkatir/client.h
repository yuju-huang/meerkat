// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/meerkatstore/meerkatir/client.h:
 *   Meerkatir client interface (uses meerkatir for replcation and the
 *   meerkatstore transactional storage system).
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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
 
#ifndef _MEERKATSTORE_MEERKATIR_CLIENT_H_
#define _MEERKATSTORE_MEERKATIR_CLIENT_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/common/frontend/bufferclient.h"
#include "client/client.h"

#include <memory>
#include <thread>

namespace meerkatstore {
namespace meerkatir {

class Client {
public:
    Client(int nsthreads, int nShards, uint32_t id,
           std::shared_ptr<zip::client::client> client, zip::network::buffer&& buffer);

    // Overriding functions from ::Client.
    void Begin();
    int Get(const std::string &key, std::string &value);
    // Interface added for Java bindings
    std::string Get(const std::string &key);
    int Put(const std::string &key, const std::string &value);
    bool Commit();
    void Abort();
    std::vector<int> Stats();

public:
    // Returns the underlying read and write set.
    const Transaction& GetTransaction() const { return txn; }

private:
    // Prepare function
    int Prepare();

private:
    // Unique ID for this client.
    const uint64_t client_id;

    // Transaction to keep track of read and write set.
    Transaction txn;

    // Ongoing transaction ID.
    uint64_t t_id;

    // Ziplog data structures
    std::shared_ptr<zip::client::client> ziplogClient;
    zip::network::buffer ziplogBuffer;
};

} // namespace meerkatir
} // namespace meerkatstore

#endif /* _MEERKATSTORE_MEERKATIR_CLIENT_H_ */
