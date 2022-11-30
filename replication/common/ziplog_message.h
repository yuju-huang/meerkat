#pragma once

#include "api/api.h"

/**
 * Message types.
 */
enum zipkat_message_type {
    COMMIT_RESPONSE = 21,
};

/**
 * A struct for client insert ack.
 */
struct commit_response: zip::api::message<commit_response> {

    /** type tag for this message */
    static constexpr uint64_t tag = COMMIT_RESPONSE;

    // Same as consensus_response_t
    uint64_t req_nr;
    uint64_t txn_nr;
    uint64_t replicaid;
    uint64_t view;
    int status;
    bool finalized;
} __attribute__((packed));
