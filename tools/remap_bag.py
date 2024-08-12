#!/usr/bin/python

import rospy
import rosbag
import os
import sys
import argparse
import yaml

def add_prefix(inbag, outbag, prefix):
    """
    Add a prefix to frame IDs, topic names, and write to a new bag file.

    Args:
        inbag (str): Path to the input bagfile.
        outbag (str): Path to the output bagfile.
        prefix (str): The prefix to add to frame IDs and topic names.

    Returns:
        None
    """
    # Log information about the input bagfile, output bagfile, and prefix
    print("Processing input bagfile: %s" % inbag)
    print("Writing to output bagfile: %s" % outbag)
    print("Adding prefix: %s" % prefix)

    # Create the output bagfile
    outbag = rosbag.Bag(outbag, 'w')

    # Iterate through messages in the input bagfile
    for topic, msg, t in rosbag.Bag(inbag, 'r').read_messages():
        if topic == "/tf":
            new_transforms = []
            for transform in msg.transforms:
                # For /tf messages, add prefix to each header frame ID and child frame ID
                if transform.header.frame_id[0] == '/':
                    transform.header.frame_id = prefix + transform.header.frame_id
                else:
                    transform.header.frame_id = prefix + '/' + transform.header.frame_id

                if transform.child_frame_id[0] == '/':
                    transform.child_frame_id = prefix + transform.child_frame_id
                else:
                    transform.child_frame_id = prefix + '/' + transform.child_frame_id

                new_transforms.append(transform)
            msg.transforms = new_transforms
        else:
            try: 
                # Add prefix to header frame ID
                if msg.header.frame_id[0] == '/':
                    msg.header.frame_id = prefix + msg.header.frame_id
                else:
                    msg.header.frame_id = prefix + '/' + msg.header.frame_id
            except:
                pass

        # Add prefix to topic name
        if topic != "/tf":
            if topic[0] == '/':
                topic = prefix + topic
            else:
                topic = prefix + '/' + topic

        # Write the message to the output bagfile
        outbag.write(topic, msg, t)

    # Close the output bagfile
    print('Closing output bagfile and exit...')
    outbag.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Add a prefix to frame IDs, topic names, and write to a new bag file.')
    parser.add_argument('-i', metavar='INPUT_BAGFILE', required=True, help='input bagfile')
    parser.add_argument('-o', metavar='OUTPUT_BAGFILE', required=True, help='output bagfile')
    parser.add_argument('-p', metavar='PREFIX', required=True, help='prefix to add to the frame ids, use / at the beginning')
    args = parser.parse_args()

    try:
        add_prefix(args.i,args.o,args.p)
    except Exception as e:
        import traceback
        traceback.print_exc()