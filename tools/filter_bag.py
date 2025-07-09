#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rosbag
import argparse
import sys
from tqdm import tqdm

def filter_rosbag(input_bag_path, output_bag_path, frames_to_keep, decimation_ratio):
    """
    过滤ROSbag文件，包括过滤/tf topic的frame和对所有topic进行抽帧。

    :param input_bag_path: 输入的rosbag文件路径。
    :param output_bag_path: 输出的rosbag文件路径。
    :param frames_to_keep: 一个列表，包含/tf topic中需要保留的frame_id和child_frame_id。
    :param decimation_ratio: 抽帧比率 N，即每 N 帧保留 1 帧。
    """
    if decimation_ratio < 1:
        print("错误：抽帧比率必须大于或等于1。")
        sys.exit(1)

    print(f"开始处理 bag 文件: {input_bag_path}")
    print(f"将要保留的 /tf frames: {frames_to_keep}")
    print(f"所有 Topic 的抽帧比率: {decimation_ratio} (每 {decimation_ratio} 帧取 1 帧)")

    # 使用集合(set)以获得更快的查找速度
    frames_set = set(frames_to_keep)
    
    # 为每个topic维护一个消息计数器
    topic_counters = {}

    try:
        with rosbag.Bag(output_bag_path, 'w') as out_bag:
            with rosbag.Bag(input_bag_path, 'r') as in_bag:
                # 获取总消息数以用于进度条
                total_messages = in_bag.get_message_count()
                print(f"总消息数: {total_messages}")

                # 使用tqdm创建进度条
                for topic, msg, t in tqdm(in_bag.read_messages(), total=total_messages, desc="正在处理消息"):
                    
                    # 1. 初始化或获取当前topic的计数器
                    if topic not in topic_counters:
                        topic_counters[topic] = 0
                    
                    msg_to_write = None

                    # 2. 对 /tf topic 进行 frame 过滤
                    if topic == '/tf':
                        # 创建一个新的TFMessage消息，只包含需要保留的transform
                        # msg的类型通常是 tf2_msgs.msg.TFMessage
                        filtered_tf_msg = type(msg)() 
                        
                        # 遍历原始消息中的所有transform
                        for transform in msg.transforms:
                            # 如果transform的父或子frame在我们的保留列表中，则保留它
                            if transform.header.frame_id in frames_set or \
                               transform.child_frame_id in frames_set:
                                filtered_tf_msg.transforms.append(transform)
                        
                        # 如果过滤后仍然有transform剩下，则这条消息有资格被写入
                        if filtered_tf_msg.transforms:
                            msg_to_write = filtered_tf_msg
                        else:
                            # 如果所有transform都被过滤掉了，则跳过这条/tf消息
                            continue
                    else:
                        # 其他topic的消息直接进入下一步
                        msg_to_write = msg

                    # 3. 对所有topic进行抽帧过滤
                    # 检查当前消息的索引是否符合抽帧比率
                    if topic_counters[topic] % decimation_ratio == 0:
                        out_bag.write(topic, msg_to_write, t)

                    # 无论是否写入，都增加该topic的计数器
                    topic_counters[topic] += 1

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        sys.exit(1)

    print("\n处理完成！")
    print(f"已生成过滤后的 bag 文件: {output_bag_path}")


if __name__ == '__main__':
    # --- 参数解析 ---
    parser = argparse.ArgumentParser(description="过滤ROSbag文件，筛选/tf topic的frames并对所有topic进行抽帧。")
    
    parser.add_argument('-i', '--input', required=True,
                        help="输入的ROSbag文件路径。")
    parser.add_argument('-o', '--output', required=True,
                        help="过滤后输出的ROSbag文件路径。")
    parser.add_argument('-f', '--frames', required=True, nargs='+',
                        help="需要保留的/tf frame名称列表，用空格分隔。例如: map odom base_link")
    parser.add_argument('-n', '--ratio', required=True, type=int,
                        help="抽帧比率N (一个正整数)，表示每N帧保留1帧。")

    args = parser.parse_args()

    # --- 调用主函数 ---
    filter_rosbag(args.input, args.output, args.frames, args.ratio)