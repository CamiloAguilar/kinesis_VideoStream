from boto import kinesis
import time


kinesis = kinesis.connect_to_region("us-west-2")
stream_name = "TestStream"
tries = 0
while tries < 100:
    tries += 1
    try:
        response = kinesis.describe_stream(stream_name)
        #print(response)
        if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
            print("stream is active")
            shards = response['StreamDescription']['Shards']
            for shard in shards:
                shard_id = shard["ShardId"]
                print (repr(shard))
                shard_it =  kinesis.get_shard_iterator(stream_name, shard_id, "LATEST")["ShardIterator"]
                while True:
                    out = kinesis.get_records(shard_it, limit=2)
                    for o in out["Records"]:
                        print (o["Data"])
                        # You specific data processing goes here

                    shard_it = out["NextShardIterator"]
    except:
            print('error while trying to describe kinesis stream : %s')