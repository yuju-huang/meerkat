#pragma once

#include <string>

#define GET_TIMEOUT 250
#define PUT_TIMEOUT 250
#define PREPARE_TIMEOUT 1000

// TODO: refine
const std::string kOrderAddr = "192.168.99.16:5813";
constexpr int kZiplogShardId = 0;
constexpr int kServerPort = 51736;
const std::string kServerIp = "192.168.99.17";
constexpr size_t kZiplogClientRate = 10000;
