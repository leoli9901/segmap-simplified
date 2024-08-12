#!/usr/bin/env python

import argparse

import rospy
import rosbag
from sensor_msgs.msg import PointCloud2
import sensor_msgs.point_cloud2 as pc2


def read_pointcloud_data(bag_file, topic_name=None, pointcloud_msg_type='sensor_msgs/PointCloud2', num_msg=None, msg_to_read=None, header_only=False):
    with rosbag.Bag(bag_file, 'r') as bag:
        bag_info = bag.get_type_and_topic_info()
        pointcloud_topics = list()
        for name, topic_tuple in bag_info.topics.items():
            if topic_tuple.msg_type == pointcloud_msg_type:
                pointcloud_topics.append(name)
        
        if topic_name:
            if topic_name in pointcloud_topics:
                pointcloud_topics = [topic_name]
            else:
                raise ValueError("Topic '{}' with msg type '{}' not found".format(topic_name, pointcloud_msg_type))
        else:
            if len(pointcloud_topics) != 0:
                print("Found topic(s) {} with msg type '{}'".format(pointcloud_topics, pointcloud_msg_type))
            else:
                raise ValueError("No topic with msg type '{}' found".format(pointcloud_msg_type))
                
        msg_no = 0
        for topic, msg, t in bag.read_messages(topics=pointcloud_topics):
            if msg_to_read is None or msg_no in msg_to_read:
                print("Header {}:\n{}".format(msg_no, msg.header))
                if not header_only:
                    gen = pc2.read_points(msg)
                    print("Points {}:\n".format(msg_no))
                    for p in gen:
                        print(p)
            
            msg_no += 1
            if num_msg is not None and msg_no == num_msg:
                break
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Read pointcloud messages from a ROS bag file.'
    )
    parser.add_argument('inbag', help='input bagfile (required)')
    parser.add_argument('--topic', help='specify name of point cloud topic')
    parser.add_argument('--pointcloud_msg_type', help='specify type of point cloud message', default='sensor_msgs/PointCloud2')
    parser.add_argument('--num_msg', help='read first N messages only', type=int)
    parser.add_argument('--msg_to_read', help='specify message X, Y, ... to read', nargs='+', type=int)
    parser.add_argument('--header_only', help='print header only', action='store_true')

    try:
        args = parser.parse_args()
        read_pointcloud_data(args.inbag, args.topic, args.pointcloud_msg_type, args.num_msg, args.msg_to_read, args.header_only)
    except Exception as e:
        print(e)




