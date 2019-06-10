#from lib.users import Users
import json
from boto import kinesis

import cv2
import numpy as np
import socket
import sys
import pickle as cPickle
import struct
from multiprocessing import Pool
import pytz ## for timezone calculations
import datetime

aws_region = "us-west-2"
stream_name = "TestStream"
cap=cv2.VideoCapture(0)
#pool = Pool(processes=2)

kinesis = kinesis.connect_to_region(aws_region)

camera_index = 0 # 0 is usually the built-in webcam
capture_rate = 30 # Frame capture rate.. every X frames. Positive integer.
rekog_max_labels = 123
rekog_min_conf = 50.0
frame_count = 0


## Old code
#************************************************************************
#for line in x.iter_lines():
#        kinesis = kinesis.connect_to_region(aws_region)
#        kinesis.put_record(stream_name, line, "partitionkey")
#        if line:
#            print (line)
#************************************************************************

#Send frame to Kinesis stream
def encode_and_send_frame(frame, frame_count, enable_kinesis=True, enable_rekog=False, write_file=False):
    try:
        #convert opencv Mat to jpg image
        #print "----FRAME---"
        retval, buff = cv2.imencode(".jpg", frame)

        img_bytes = bytearray(buff)

        utc_dt = pytz.utc.localize(datetime.datetime.now())
        now_ts_utc = (utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

        frame_package = {
            'ApproximateCaptureTime' : now_ts_utc,
            'FrameCount' : frame_count,
            'ImageBytes' : img_bytes
        }

        if write_file:
            print("Writing file img_{}.jpg".format(frame_count))
            target = open("img_{}.jpg".format(frame_count), 'w')
            target.write(img_bytes)
            target.close()

        # Put encoded image in kinesis stream
        if enable_kinesis:
            print("....Sending image to Kinesis")
            response = kinesis.put_record(
            	stream_name = stream_name,  # StreamName in boto3
            	data=cPickle.dumps(frame_package), # Data in boto3
            	partition_key="partitionkey" # PartitionKey in boto3
            	)
            print('Response: \n', response)

        if enable_rekog:
            response = rekog_client.detect_labels(
                Image={
                    'Bytes': img_bytes
                },
                MaxLabels=rekog_max_labels,
                MinConfidence=rekog_min_conf
            )
            print(response)

    except Exception as e:
        print(e)

while True:
    try:
        grabbed, frame = cap.read()  # grab the current frame
        frame = cv2.resize(frame, (640, 480))  # resize the frame

        if frame_count % capture_rate == 0:
        	#result = pool.apply_async(encode_and_send_frame(frame, frame_count, enable_kinesis=True, enable_rekog=False, write_file=False))
        	result = encode_and_send_frame(frame, frame_count, enable_kinesis=True, enable_rekog=False, write_file=False)
        	print('...sending 1/30 frames to AWS')

        frame_count += 1

        # Display the resulting frame
        cv2.imshow('frame', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    except KeyboardInterrupt:
        camera.release()
        cv2.destroyAllWindows()
        break

# close any open windows
cv2.destroyAllWindows()