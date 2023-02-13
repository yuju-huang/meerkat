from collections import defaultdict
from meerkat_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, ziplog_order_servers, ziplog_storage_servers, clients)
from sheets_query import header
import benchmark


"""
    The scalability graph.
"""

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        exp_type=None,
        config_file_directory=args.config_file_directory,
        f=1,
        key_file=args.key_file,
        num_keys=None,
        num_server_threads=None,
        repl_scheme=None,
        ziplog_order_binary=args.ziplog_order_binary,
        ziplog_order_port=args.ziplog_order_port,
        ziplog_order_cpus=args.ziplog_order_cpus,
        ziplog_storage_binary=args.ziplog_storage_binary,
        client_cpus=None,
        subscriber_cpus=None,
        client_binary=args.client_binary,
        client_rate=None,
#        benchmark_duration_seconds=15,
#        benchmark_warmup_seconds=5,
#        benchmark_duration_seconds=80,
#        benchmark_warmup_seconds=35,
        benchmark_duration_seconds=150,
        benchmark_warmup_seconds=60,
        transaction_length=1,
        write_percentage=0,
        zipf_coefficient=None,
        num_client_machines=None,
        num_clients_per_machine=1,
        num_threads_per_client=None,
        num_fibers_per_client_thread=None,
        suite_directory=args.suite_directory,
    )

    base_rate = 65536
    base_num_keys = 500000
    parameters_list = [
      base_parameters._replace(
          exp_type = exp_type,
          num_server_threads = num_server_threads,
          client_cpus=client_cpus,
          subscriber_cpus=subscriber_cpus,
          num_keys = num_keys,
          num_client_machines = num_client_machines,
          num_threads_per_client = num_threads_per_client,
          num_fibers_per_client_thread = num_fibers_per_client_thread,
          client_rate = client_rate,
          zipf_coefficient = zipf_coefficient,
      )

      for zipf_coefficient in [0]
      #for zipf_coefficient in [0, 0.5, 1]

      # Keep load 24 clients per core
      for (num_server_threads,
           client_cpus,
           subscriber_cpus,
           num_keys,
           num_client_machines,
           num_threads_per_client,
           num_fibers_per_client_thread,
           client_rate) in [
                                       # baseline, (p50, p99, p999) = (25,49,59)
                                       # How many clients can one server cpu handle? 
                                       # 1-3 is same as 1, 4 is ok but drop performance a bit
                                       # 1: 27799.5, 30    70  80
                                       # 2: 56412.53333, 30 70  81
                                       # 3: 81271.96667, 31  72  86
                                       # 4: 96893.1, 35 82  97
                                       # 8: 119080.7, 55  138 161
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 2, 2 * base_rate),
                                       # 4 * base_rate is the best
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 3, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 5 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 6 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 6, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 6, 6 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 8, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 8, 6 * base_rate),

                                       # rate for 4 clients, 4 is the best
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 2 * base_num_keys, 1, 1, 4, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 1, 1, 4, 4 * base_rate),
                                       # 2, 4, 8 better than 1 * base_rate, 4 is slightly better than 2, 8
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 2 * base_num_keys, 1, 1, 8, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 1, 1, 8, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 1, 1, 8, 8 * base_rate),

                                       # how many fiber per ziplog client
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 1, base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 2, 2 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 2, 3 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 2, 4 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 3, 3 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 3, 4 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 3, 5 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 3, 6 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 4 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 6 * base_rate),
                                       (1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 1, 1, 4, 8 * base_rate),

                                       # no thread
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", base_num_keys, 8, 1, 1, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 2 * base_num_keys, 8, 1, 2, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 2 * base_num_keys, 8, 1, 3, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 8, 1, 4, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 8, 1, 4, 5 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 8, 1, 4, 6 * base_rate),

                                       ## no fiber
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 8, 4, 1, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19", "21,23,25,27,29,31", 3 * base_num_keys, 8, 4, 1, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "0,2,4,6,8,10,12,14", 3 * base_num_keys, 8, 4, 1, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30", 3 * base_num_keys, 8, 4, 1, base_rate),

                                       ## 3 fibers
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "0,2,4,6,8,10,12,14,16,38,20,22,24,26,28,30", 8 * base_num_keys, 8, 4, 3, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,0,2,4,6", "8,10,12,14,16,18,20,22,24,26,28,30", 8 * base_num_keys, 8, 4, 3, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,0,2,4,6,8,10,12,14", "16,18,20,22,24,26,28,30", 8 * base_num_keys, 8, 4, 3, 4 * base_rate),

                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 3 * base_num_keys, 8, 4, 1, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 6 * base_num_keys, 8, 4, 2, base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 12 * base_num_keys, 8, 4, 4, 1 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 12 * base_num_keys, 8, 4, 4, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 12 * base_num_keys, 8, 4, 4, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 24 * base_num_keys, 8, 4, 8, 1 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 24 * base_num_keys, 8, 4, 8, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 24 * base_num_keys, 8, 4, 8, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 24 * base_num_keys, 8, 4, 8, 8 * base_rate),

                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 12 * base_num_keys, 8, 4, 4, 1 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 12 * base_num_keys, 8, 4, 4, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 12 * base_num_keys, 8, 4, 4, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 12 * base_num_keys, 8, 4, 4, 8 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 24 * base_num_keys, 8, 4, 8, 1 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 24 * base_num_keys, 8, 4, 8, 2 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 24 * base_num_keys, 8, 4, 8, 4 * base_rate),
                                       #(1, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59", 24 * base_num_keys, 8, 4, 8, 8 * base_rate),
                                       #(1, "1", "17", 1 * base_num_keys, 1, 1, 1, base_rate),
                                       #(1, "1", "3", 2 * base_num_keys, 2, 1, 1, base_rate),
                                       #(1, "1", "17", 3 * base_num_keys, 3, 1, 1, base_rate),
                                       #(1, "1", "17", 4 * base_num_keys, 4, 1, 1, base_rate),
                                       #(1, "1", "3", 8 * base_num_keys, 8, 1, 1, 0.5 * base_rate),
                                       #(1, "1", "3", 8 * base_num_keys, 8, 1, 1, base_rate),
                                       #(1, "1", "3", 16 * base_num_keys, 8, 2, 1, base_rate),

                                       #(2, "1,3,5,7", "17,19,21,23", 4 * base_num_keys, 4, 1, 1, base_rate),
                                       #(2, "1,3", "17,19", 4 * base_num_keys, 4, 1, 1, base_rate),
                                       #(2, "1,3", "17", 4 * base_num_keys, 4, 1, 1, base_rate),
                                       #(2, "1", "17", 4 * base_num_keys, 4, 1, 1, base_rate),

                                       #(2, "1,3,5,7,9,11,13,15", "17,19,21,23", 8 * base_num_keys, 8, 1, 1, base_rate),
                                       #(2, "1,3,5,7", "17,19,21,23", 8 * base_num_keys, 8, 1, 1, base_rate),
                                       #(2, "1,3,5,7", "17,19", 8 * base_num_keys, 8, 1, 1, base_rate),
                                       #(2, "1,3", "17,19", 8 * base_num_keys, 8, 1, 1, base_rate),

                                       # Evaluation
                                       #(1, "1", "3", 1 * base_num_keys, 1, 1, 1, base_rate),
                                       #(2, "1", "3", 2 * base_num_keys, 2, 1, 1, base_rate),
                                       #(4, "1,3", "5", 4 * base_num_keys, 4, 1, 1, base_rate),
                                       #(8, "1,3,5,7", "9,11", 8 * base_num_keys, 8, 1, 1, base_rate),

                                       #(10, "1,3,5,7,9,11,13,15,17,19", "21,23,25,27,29,31,33,35,37,39", 10 * base_num_keys, 10, 1, 1, base_rate),
                                       #(10, "1,3,5,7", "9,11,13,15", 10 * base_num_keys, 10, 1, 1, base_rate),
                                       #(10, "1,3,5,7", "9,11", 10 * base_num_keys, 10, 1, 1, base_rate),
                                       #(12, "1,3,5,7,9,11,13,15,17,19,21,23", "25,27,29,31,33,35,37,39,41,43,45,47", 12 * base_num_keys, 6, 2, 1, base_rate),
                                       #(12, "1,3,5,7,9,11", "13,15,17,19,21,23", 12 * base_num_keys, 6, 2, 1, base_rate),
                                       #(12, "1,3,5,7,9,11", "13,15,17", 12 * base_num_keys, 6, 2, 1, base_rate),
                                       #(12, "1,3,5,7", "9,11,13,15", 12 * base_num_keys, 6, 2, 1, base_rate),
                                       #(12, "1,3,5,7", "9,11", 12 * base_num_keys, 6, 2, 1, base_rate),

                                       #(14, "1,3,5,7,9,11,13,15,17,19,21,23,25,27", "29,31,33,35,37,39,41,43,45,47,49,51,53,55", 14 * base_num_keys, 7, 2, 1, base_rate),
                                       #(14, "1,3,5,7,9,11,13", "15,17,19,21,23,25,27", 14 * base_num_keys, 7, 2, 1, base_rate),
                                       #(14, "1,3,5,7,9,11", "13,15,17,19,21,23", 14 * base_num_keys, 7, 2, 1, base_rate),
                                       #(14, "1,3,5,7,9,11", "13,15,17,19", 14 * base_num_keys, 7, 2, 1, base_rate),
                                       #(14, "1,3,5,7,9,11", "13,15,17", 14 * base_num_keys, 7, 2, 1, base_rate),
                                    
                                       # 16 clients, it performs similarly
                                       #(16, "1,3,5,7,9,11", "13,15,17,19", 16 * base_num_keys, 4, 4, 1, base_rate),
                                       #(16, "1,3,5,7,9,11", "13,15,17,19", 16 * base_num_keys, 8, 2, 1, base_rate),
                                       #(16, "1,3,5,7,9,11", "13,15,17", 16 * base_num_keys, 8, 2, 1, base_rate),

                                       #(32, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15,17,19,21,23", "25,27,29,31,33,35", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15,17,19,21,23", "25,27,29,31,33,35", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,31", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15", "17,19,21,23", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(64, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41", "43,45,47,49,51,53,55,57,59,61,63", 64 * base_num_keys, 8, 8, 1, base_rate),

                                       #(16, "1,3,5,7", "9,11", 16 * base_num_keys, 8, 2, 1, base_rate),
                                       #(32, "1,3,5,7,9,11,13,15", "17,19,21,23", 32 * base_num_keys, 8, 4, 1, base_rate),
                                       #(64, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 64 * base_num_keys, 8, 8, 1, base_rate),
                                       #(96, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47", "49,51,53,55,57,59,61,63", 96 * base_num_keys, 8, 12, 1, base_rate),

                                       # client_cpu, subscriber_cpu ratio -> 2:1 seems the best
                                       #(8, "1,3,5,7,9,11,13,15", "17", 8 * base_num_keys, 4, 2, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19", 8 * base_num_keys, 4, 2, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19,21,23", 8 * base_num_keys, 4, 2, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 8 * base_num_keys, 4, 2, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17", 8 * base_num_keys, 8, 1, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19", 8 * base_num_keys, 8, 1, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19,21,23", 8 * base_num_keys, 8, 1, 1, 65536),
                                       #(8, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 8 * base_num_keys, 8, 1, 1, 65536),

                                       #(16, "1,3,5,7,9,11,13,15", "17", 16 * base_num_keys, 8, 2, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19", 16 * base_num_keys, 8, 2, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19,21,23", 16 * base_num_keys, 8, 2, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 16 * base_num_keys, 8, 2, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19,21,23", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15", "17,19,21,23,25,27,29,31", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23", "25,27,29,31", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63", 16 * base_num_keys, 4, 4, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 16 * base_num_keys, 8, 2, 1, 65536),
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 16 * base_num_keys, 4, 4, 1, 65536),
                                       # Use hyperthread -> bad
                                       #(16, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "2,4,6,8,10,12,14,16", 16 * base_num_keys, 4, 4, 1, 65536),

                                       #(32, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 16 * base_num_keys, 8, 4, 1, 65536),
                                       #(64, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31", "33,35,37,39,41,43,45,47", 16 * base_num_keys, 8, 4, 1, 65536),
                                       #(64, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41", "43,45,47,49,51,53,55,57,59,61,63", 16 * base_num_keys, 8, 4, 1, 65536),
                                       #(64, "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39", "41,43,45,47,49,51,53,55,57,59,61,63", 16 * base_num_keys, 8, 4, 1, 65536),

                                       #(1, 500000, 1, 1, 1, 100000),
                                       #(1, 500000, 1, 1, 1, 200000),
                                       #(1, 500000, 1, 1, 1, 400000),
                                       #(1, 500000, 1, 1, 1, 800000),
                                       #(1, 500000, 1, 1, 1, 1600000),

                                       #(2, 2 * 500000, 1, 1, 2, 2 * base_rate),
                                       #(4, 4* 500000, 1, 1, 4, 4 * base_rate),
                                       #(8, 8 * 500000, 1, 1, 8, 8 * base_rate),
                                       #(16, 16 * 500000, 1, 1, 16, 16 * base_rate),

                                       #(1, 500000, 1, 1, 1),
                                       #(2, 2 * 500000, 2, 1, 1),
                                       #(4, 4* 500000, 4, 1, 1),
                                       #(8, 8 * 500000, 8, 1, 1),
                                       #(16, 16 * 500000, 8, 2, 1),
                                       #(32, 32 * 500000, 8, 4, 1),

                                       #(1, 500000, 1, 1, 1),
                                       #(2, 500000, 2, 1, 1),
                                       #(4, 500000, 4, 1, 1),
                                       #(8, 500000, 8, 1, 1),
                                       #(16, 500000, 8, 2, 1),
                                       #(32, 500000, 8, 4, 1),
                                       #(64, 500000, 8, 8, 1),

                                       #(4, 4 * 500000, 4, 4, 1),
                                       #(4, 4 * 500000, 2, 1, 1),
                                       #(4, 4 * 500000, 4, 1, 1),
                                       #(4, 4 * 500000, 8, 8, 1),
                                       #(4, 4 * 500000, 2, 1, 1),
                                       #(4, 4 * 500000, 1, 2, 1),
                                       #(4, 4 * 500000, 4, 4, 1),
                                       #(4, 4 * 500000, 1, 4, 1),
                                       #(4, 4 * 500000, 8, 1, 1),
                                       #(4, 4 * 500000, 1, 8, 1),
                                       #(4, 4 * 500000, 1, 4, 1),
                                       #(4, 4 * 500000, 4, 8, 1),
                                       #(8, 8 * 500000, 2, 6, 8),
                                       #(16, 16 * 500000, 4, 6, 8),
                                       #(32, 32 * 500000, 8, 6, 8),
                                       #(64, 64 * 500000, 10, 8, 8),
                                       #(68, 68 * 500000, 10, 11, 8),
                                       #(72, 72 * 500000, 10, 11, 8),
                                       #(76, 76 * 500000, 10, 12, 8),
                                       #(78, 78 * 500000, 10, 12, 8),
                                       #(78, 78 * 500000, 10, 12, 8),
                                       #(80, 80 * 500000, 8, 12, 8),
                                       #(80, 80 * 500000, 10, 12, 8),
                                       ]

      for exp_type in ["client" + str(num_client_machines * num_threads_per_client * num_fibers_per_client_thread) + "_zipf" + str(zipf_coefficient) + "_keys" + str(num_keys)]
    ]

    # Run every experiment three times.
    #parameters_list = [q for p in parameters_list for q in [p] * 3]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e1_and_e2')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, clients(), ziplog_order_servers(), ziplog_storage_servers())

if __name__ == '__main__':
    main(parse_args())
