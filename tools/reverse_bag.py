#!/usr/bin/python3
import rospy
import rosbag
import time
from os import path
from tqdm import tqdm
import copy

def reverse_timestamp(input_path, output_path, output_reversed_path):
    try:
        print("Opening bag files ...", end=' ')
        inbag = rosbag.Bag(input_path, 'r')
        outbag_adjusted = rosbag.Bag(output_path, 'w')
        outbag_reversed = rosbag.Bag(output_reversed_path, 'w')
        print("done.")

        start_time = inbag.get_start_time()
        end_time = inbag.get_end_time()
        adjusted_msg_list = []
        reversed_msg_list = []      
        # 外部timestamp -> 内部timestamp
        for topic, msg, t in tqdm(inbag.read_messages(), total=inbag.get_message_count(), desc='Reversing timestamps of input bagfile: ' + path.split(input_path)[1]):
            if topic == '/tf' and msg.transforms: # 一条tf消息中有多个transform的, 以其中最大 (最晚) 的对齐外部timestamp, 其他transform按照相对该时间戳的offset调整
                max_t = msg.transforms[0].header.stamp.to_sec()
                for tf in msg.transforms: 
                    if max_t < tf.header.stamp.to_sec():
                        max_t = tf.header.stamp.to_sec()
                for tf in msg.transforms:
                    tf.header.stamp = rospy.Time.from_sec(tf.header.stamp.to_sec() - max_t + t.to_sec())
            elif msg._has_header: # 假定/tf以外的其他message中只有一个timestamp
                msg.header.stamp = t
            adjusted_msg_list.append((topic, msg, t))
            
        # 逐一修改外部timestamp, 达到相对原有数据倒序的效果
            new_t = rospy.Time.from_sec(end_time + start_time - t.to_sec())
            new_msg = copy.deepcopy(msg)
            if topic == '/tf' and new_msg.transforms: 
                for tf in new_msg.transforms:
                    tf.header.stamp = rospy.Time.from_sec(end_time + start_time - tf.header.stamp.to_sec())
            elif new_msg._has_header: # 假定/tf以外的其他message中只有一个timestamp
                new_msg.header.stamp = new_t
            reversed_msg_list.append((topic, new_msg, new_t))

        # 排序以确保时间戳递增
        print("Sorting messages by timestamp ...", end=' ')
        adjusted_msg_list.sort(key=lambda x: x[2].to_sec())
        reversed_msg_list.sort(key=lambda x: x[2].to_sec())
        print("done.") 
        
        for topic, msg, t in tqdm(adjusted_msg_list, total=adjusted_msg_list.__len__(), desc='Writing adjusted bagfile: ' + path.split(output_path)[1]):
            outbag_adjusted.write(topic, msg, t)
        for topic, msg, t in tqdm(reversed_msg_list, total=reversed_msg_list.__len__(), desc='Writing reversed bagfile: ' + path.split(output_reversed_path)[1]):
            outbag_reversed.write(topic, msg, t)    
        
                    
    finally:
        print("Closing bag files ...", end=' ')
        inbag.close()
        outbag_adjusted.close()
        outbag_reversed.close()
        print("done.")
            
                
if __name__ == '__main__':
    input_bag_path = 'drive_18_170_287_na3.bag'
    output_bag_path = 'drive_18_170_287_na3_adjusted.bag'
    output_reversed_path = 'drive_18_170_287_na3_reversed.bag'
    
    reverse_timestamp(input_bag_path, output_bag_path, output_reversed_path)