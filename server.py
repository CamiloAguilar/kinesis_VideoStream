import boto3
import time
import cv2
import numpy as np
import pickle as cPickle

aws_region = "us-west-2"
stream_name = "TestStream"

#kinesis = kinesis.connect_to_region("us-west-2") with boto
kinesis = boto3.client('kinesis', region_name = aws_region) # with boto3
stream_name = "TestStream"

while True:
    try: 
        response = kinesis.describe_stream(StreamName = stream_name)
        if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
            print("stream is active")
            shards = response['StreamDescription']['Shards']
            for shard in shards:
                shard_id = shard["ShardId"]
                #print (repr(shard))
                shard_it =  kinesis.get_shard_iterator(
                    StreamName = stream_name, 
                    ShardId = shard_id, 
                    ShardIteratorType = "LATEST")["ShardIterator"]
                
                out = kinesis.get_records(
                    ShardIterator = shard_it, 
                    Limit=1)['Records'][0]['Data']
                    
                # Unpack the frame
                frame = cPickle.loads(out)['ImageBytes']
                frame = np.asarray(frame, dtype=np.uint8)
                frame = frame.reshape((frame.shape[0], 1))
                newframe = cv2.imdecode(frame, cv2.COLOR_BGR2RGB)

                # Display
                cv2.imshow('Stream_frame', newframe)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
                    
    except Exception as e:
        print('... error while trying to describe kinesis stream : %s')
        print(e)

    

# release camera
camera.release()
cv2.destroyAllWindows()

# close any open windows
cv2.destroyAllWindows()