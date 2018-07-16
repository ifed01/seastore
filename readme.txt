This is a small POC project intended to learn basic SeaStar (https://github.com/scilladb/seastar) C++ framework capabilities.
It simulates server side that listens and accepts multiple incoming TCP connections on port 1234 and handles them in a 'sharded' and async manner:

- Each incoming request belongs to specific 'virtual' collection indentified by corresponding integer identifier. 
- Any TCP connection can transport any virtual collection.
- Every request is processed at server side in an async manner.
- Request processing needs some time to complete. Currently this is simulated by a sleep call. The duration(in ms) is determined by the corresponding feield within a request. During this period all other requests for this specific virtual collection must wait. But other collections are free to run in parallel. 
- Each specific collection is always processed by corresponding Shard which is an entity running at specific CPU core. Amount of Shards is equal to CPU count.
- Each Shard can handle multiple collections though. Sharding is perfromed trivially using shard_no = col_id % CPU_count formula
- Incoming TCP connections are handled by all CPU cores which is provided by SeaStar automatically. But sharding is different and implicit here.
- Hence each received request has to be forwarded to corresponding Shard (and its working thread) and either to be sheduled for execution or put on hold using per-collection queue.
- Just a single request for each collection is allowed to run at the given moment of time. However requests from multiple collections are run in parallel.

To simulate client side one can use telnet client connecting to the above server at port 1234.
To issue a request it should send the following string: req <arbitrary_req_name> <col_id> <duration>
No replies are currently provided.
To stop a server one can issue 'stop' command over any active connection.


