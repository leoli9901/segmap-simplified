#!/usr/bin/python2
#coding=utf-8
"""
使用Python2打开此脚本, Python3可能有问题
输入包括:
    1. KITTI格式/符合SegMap输入格式要求的rosbag, 其中至少包含以下两个topic:
        1) sensor_msgs/PointCloud2格式的点云topic,
        2) /tf (ROS中保存位姿的topic), 其中需要包含描述机器人运动轨迹的位姿序列.
    2. 为了估算机器人运动轨迹的长度, 还需要输入/tf中静止参考系与移动机器人坐标系的的名称 (ROS中的坐标系用frame表示), 对于KITTI数据集, 输入为'world' (世界坐标系) 与'imu' (IMU坐标系, 即车辆坐标系).
脚本对输入的bag进行如下操作:
    1. 根据输入的一对frame, 通过累加/tf中这对frame相邻两条message之间(对KITTI数据间隔约0.1s)的相对位移, 统计机器人运动轨迹长度随时间增长的情况, 并估算输入rosbag中机器人全过程运动轨迹的长度.
    2. 对输入rosbag进行分段; 默认的自动分段模式依据上一步估算的轨迹长度, 按照输入的段数及首尾重合比例进行切割, 使输出的每一段轨迹长度大致相同; 也支持手动切割模式, 此时要求传入YAML文件的路径, 程序直接按照文件中指定的切分段数, 每段起始时刻与终止时刻切割.
    3. 对分段后的每一段轨迹, 首先随机指定每一段轨迹为正向或反向, 反向即为将bag中的数据全部倒序, 通过修改timestamp并按照timestamp重新排序实现.
    4. 为了去除原始bag中包含的每段轨迹的相对位姿, 将/tf中这对frame的第一条message的位姿置为0, 后续message的位姿则修改为与第一条message的相对位姿.
    5. 对切割后的bag中的每一个topic, 按照SegMap的要求添加前缀, 默认为'/naX', 其中X为机器人编号 (0, 1, 2, ...); 也支持手动指定前缀.
输出包括: 
    1. 在脚本运行路径下生成符合SegMap多机回环模式要求的多个rosbag文件, 文件名为'[原文件名]_[在原bag中的起始时间]_[在原bag中的结束时间]_[前缀].bag', 若起始时间大于结束时间, 说明此段bag与原始轨迹方向相反.
    2. 在命令行输出每段轨迹的信息: 在原始bag文件中的起始时间与结束时间, 原始bag世界坐标系下起点位姿与终点位姿, 以及轨迹大致的长度.
脚本的参数包括:
    1 . 【必需】输入的rosbag文件名,
    2 . 【需根据rosbag情况手动指定】直接描述机器人位姿的一对坐标系的名称, 输入时用空格分开; 默认为'world'与'imu', 对应KITTI数据集的世界坐标系与车辆坐标系,
    3a. 自动切割选项, 默认切分为4段, 首尾重合段占每段轨迹长度的10%,
    3b. 手动切割选项, 需要给出一个YAML文件路径, YAML文件中包含段数, 及每段的起始时间、结束时间 (起始时间大于结束时间时, 视为将轨迹取反),
    4 . 前缀选项, 默认为'na', 即设置前缀为'/naX', 其中X为机器编号, 
    5 . 是否保留每段轨迹的起始位姿, 默认不保留 (重设为0),
    6 . 是否保留每段轨迹原始的方向, 默认不保留 (每段轨迹随机取正向或反向), 手动切割模式下该选项无效.
"""
import argparse
import time
import math
from os import path
import copy

import rospy
import rosbag
from geometry_msgs.msg import Transform
from tf.transformations import quaternion_matrix, quaternion_from_matrix, translation_from_matrix, euler_from_quaternion

import numpy as np
import yaml
from tqdm import tqdm

def transform_to_matrix(transform):
    rot = transform.rotation
    trans = transform.translation
    q = [rot.x, rot.y, rot.z, rot.w]
    t = [trans.x, trans.y, trans.z]
    R = quaternion_matrix(q)[:3, :3]
    T = np.identity(4)
    T[:3, :3] = R
    T[:3, 3] = t
    return T

def matrix_to_transform(matrix):
    rot = quaternion_from_matrix(matrix)
    trans = translation_from_matrix(matrix)
    transform = Transform()
    transform.rotation.x = rot[0]
    transform.rotation.y = rot[1]
    transform.rotation.z = rot[2]
    transform.rotation.w = rot[3]
    transform.translation.x = trans[0]
    transform.translation.y = trans[1]
    transform.translation.z = trans[2]
    return transform

def calculate_translation(transform1, transform2):
    trans = [transform2.translation.x - transform1.translation.x,
             transform2.translation.y - transform1.translation.y,
             transform2.translation.z - transform1.translation.z]
    return trans

def calculate_relative_pose(transform1, transform2):
    # 将 geometry_msgs/Transform 转换为变换矩阵
    matrix1 = transform_to_matrix(transform1)
    matrix2 = transform_to_matrix(transform2)

    # 计算相对位姿的变换矩阵
    relative_matrix = np.dot(np.linalg.inv(matrix1), matrix2)

    # 将变换矩阵转换为 geometry_msgs/Transform
    relative_transform = matrix_to_transform(relative_matrix)

    return relative_transform

# Generate True / False, each with 0.5 probability 
def get_random_bool():
    return np.random.random() > 0.5

def transform_to_euler_and_trans(transform):
    rot = transform.rotation
    trans = transform.translation
    q = [rot.x, rot.y, rot.z, rot.w]
    t = [trans.x, trans.y, trans.z]
    euler = euler_from_quaternion(q)
    return euler, t

def cut_and_add_prefix(
        input_bag_path, 
        frame_pair=['world', 'imu'],
        num_segment=4, 
        overlap_ratio=0.1, 
        manual_cut_file=None, 
        prefix='na',
        keep_start_pose=False, 
        keep_direction=False,
        print_info=False
    ):
    try:
        inbag_filename = path.split(input_bag_path)[1]
        print("Opening {} ...".format(inbag_filename))
        inbag = rosbag.Bag(input_bag_path, 'r')
        outbag = None
        if '/tf' not in inbag.get_type_and_topic_info().topics.keys():
            raise ValueError("The input bag file does not contain /tf topic.")
    
        # Step 1: 统计机器人运动轨迹长度随时间增长的情况, 计算路径总长度
        timestamp_length = [] # [(timestamp0, cumulated_length@timestamp0), ...]
        for _, msg, timestamp in tqdm(inbag.read_messages(topics='/tf'), total=inbag.get_message_count(topic_filters='/tf'), desc='Calculating path length of ' + inbag_filename):
            if msg.transforms:
                for tf_msg in msg.transforms:
                    if tf_msg.header.frame_id == frame_pair[0] and tf_msg.child_frame_id == frame_pair[1]:
                        t = timestamp.to_sec() # 将时间戳转换为一般的float格式存储
                        if len(timestamp_length) == 0: # 处理第一个tf时的特殊情况
                            timestamp_length.append((t, 0.0))
                        else:
                            # relative_pose = calculate_relative_pose(last_tf, tf_msg.transform)
                            translation = calculate_translation(last_tf, tf_msg.transform)
                            # length = np.linalg.norm([relative_pose.translation.x, relative_pose.translation.y, relative_pose.translation.z])
                            length = np.linalg.norm(translation)
                            timestamp_length.append((t, timestamp_length[-1][1] + length))
                        last_tf = tf_msg.transform
                        break # 只处理每条/tf消息中的第一个符合要求的transform
    
        if len(timestamp_length) == 0:
            raise ValueError("The input bag file does not contain /tf topic with specified frame pair.")
        # timestamp_length = sorted(timestamp_length, key=lambda x: x[1])
        print("Complete. Path length = {:.3f}m".format(timestamp_length[-1][1]))
        
        # Step 2: 读取手动切割参数, 或计算自动切割参数
        # 读取手动切割参数
        if manual_cut_file:
            with open(manual_cut_file, 'r') as file:
                data = yaml.safe_load(file)
            num_segment = data['num_segment']
            print("Manual cutting parameters: num_segment = {}".format(num_segment))
            bag_start = inbag.get_start_time()
            bag_end = inbag.get_end_time()
            bag_duration = bag_end - bag_start
            cutting_timestamp_pair_list = []
            for i in range(num_segment):
                param = data['segment_{}'.format(i + 1)]
                start_time = param['start_time']
                end_time = param['end_time']
                if end_time > bag_duration:
                    end_time = bag_duration
                    print("Warning: segment {} end_time exceeds bag duration, set to bag duration.".format(i + 1))
                if start_time > bag_duration:    
                    start_time = bag_duration
                    print("Warning: segment {} start_time exceeds bag duration, set to bag duration.".format(i + 1))
                if start_time >= 0 and end_time >= 0 and start_time <= bag_duration and end_time <= bag_duration:
                    print(" -  [{}, {}]".format(start_time, end_time))
                    cutting_timestamp_pair_list.append((bag_start + start_time, bag_start + end_time))     
                else:
                    raise ValueError("Invalid segment {} parameters: start_time = {}, end_time = {}".format(i + 1, start_time, end_time))
                        
        # 默认的自动切割模式依据上一步估算的轨迹长度, 按照输入的切分段数及首尾重合比例进行切割, 使输出的每一段轨迹长度大致相同
        else:
            r = num_segment - overlap_ratio * (num_segment - 1) # r = total_length / length_of_each_segment
            l = timestamp_length[-1][1] / r
            print("Auto cutting parameters: num_segment = {}, overlap_ratio = {}, segment_length = {:.3f}m".format(num_segment, overlap_ratio, l))
        
            # segment 1: [0, l], segment 2 :[(1-p)l, (2-p)l], ... segment n: [((n-1)-p(n-1))l, (n-p(n-1))l]
            cutting_timestamp_pair_list = [] # [(s1_start_t, s1_end_t), (s2_start_t, s2_end_t), ...]
            cutting_length_pair_list = [] # [(s1_start_l, s1_end_l), (s2_start_l, s2_end_l), ...]
            current_segment_start_t, current_segment_start_l = timestamp_length[0]
            idx = 0
            for i in range(num_segment): # current_segment_start_t -> current_segment_end_t, next_segment_start_t
                flag = True
                while idx < len(timestamp_length):
                    if timestamp_length[idx][1] > current_segment_start_l + l:
                        current_segment_end_t, current_segment_end_l = timestamp_length[idx]
                        break
                    elif (timestamp_length[idx][1] > current_segment_start_l + (1 - overlap_ratio) * l) and flag:
                        next_segment_start_idx = idx
                        next_segment_start_t, next_segment_start_l = timestamp_length[idx]
                        flag = False
                    idx += 1
            
                if idx == len(timestamp_length):
                    current_segment_end_t, current_segment_end_l = timestamp_length[idx - 1]
        
                # 记录segment信息
                cutting_timestamp_pair_list.append((current_segment_start_t, current_segment_end_t))
                cutting_length_pair_list.append((current_segment_start_l, current_segment_end_l))
            
                # 准备下一个segment
                current_segment_start_t, current_segment_start_l = next_segment_start_t, next_segment_start_l
                idx = next_segment_start_idx
        
        # Step 3: 按照上述切割信息准备新的rosbag文件. 对每一段轨迹进行以下操作: 随机指定每一段轨迹为正向或反向, 去除原始bag中包含的每段轨迹的相对位姿, 添加前缀, 最后按照指定的文件名生成新的rosbag文件.
        for i in range(num_segment):
            start_time, end_time = cutting_timestamp_pair_list[i]
            timestamp_reverse = start_time > end_time
            if timestamp_reverse:
                start_time, end_time = end_time, start_time

            # 3.1 统一内外部timestamp, 并对轨迹随机取正反向
            print("Processing timestamp of segment {} ...".format(i + 1))
            msg_list = []
            reverse_flag = not keep_direction and get_random_bool() if manual_cut_file is None else timestamp_reverse
            for topic, msg, timestamp in inbag.read_messages(start_time=rospy.Time.from_sec(start_time), end_time=rospy.Time.from_sec(end_time)):
                # 外部timestamp -> 内部timestamp
                if topic == '/tf' and msg.transforms:
                    # 一条tf消息中有多个transform的, 以其中最大 (最晚) 的对齐外部timestamp, 其他transform按照相对该时间戳的offset调整
                    max_t = max(tf.header.stamp.to_sec() for tf in msg.transforms)
                    for tf_msg in msg.transforms:
                        tf_msg.header.stamp = rospy.Time.from_sec(tf_msg.header.stamp.to_sec() - max_t + timestamp.to_sec())
                elif msg._has_header:  
                    # 假定/tf以外的其他message中只有一个timestamp, 直接用外部timestamp赋值内部timestamp
                    msg.header.stamp = timestamp
            
                # 生成一个随机值, 若为True, 则通过对timestamp的处理得到反向的轨迹
                if reverse_flag:
                    new_t = rospy.Time.from_sec(end_time + start_time - timestamp.to_sec())
                    new_msg = copy.deepcopy(msg)
                    if topic == '/tf' and new_msg.transforms: 
                        for tf_msg in new_msg.transforms:
                            tf_msg.header.stamp = rospy.Time.from_sec(end_time + start_time - tf_msg.header.stamp.to_sec())
                    elif new_msg._has_header: 
                        new_msg.header.stamp = new_t
                
                msg_list.append((topic, new_msg if reverse_flag else msg, new_t if reverse_flag else timestamp))        
            
            # 排序以确保时间戳递增
            msg_list.sort(key=lambda x: x[2].to_sec())
            
            # '[原文件名]_[在原bag中的起始时间]_[在原bag中的结束时间]_[前缀].bag'
            if reverse_flag:
                output_bag_path = "{}_{:.0f}_{:.0f}_{}{}{}".format(path.splitext(input_bag_path)[0], end_time - inbag.get_start_time(), start_time - inbag.get_start_time(), prefix, i, path.splitext(input_bag_path)[1])
            else:
                output_bag_path = "{}_{:.0f}_{:.0f}_{}{}{}".format(path.splitext(input_bag_path)[0], start_time - inbag.get_start_time(), end_time - inbag.get_start_time(), prefix, i, path.splitext(input_bag_path)[1])
            print("Writing segment {} to {}".format(i + 1, path.split(output_bag_path)[1]))
            outbag = rosbag.Bag(output_bag_path, 'w')
            
            # 3.2 去除原始bag中包含的每段轨迹的相对位姿, 添加前缀, 最后按照指定的文件名生成新的rosbag文件
            initial_tf = None
            final_tf = None
            current_prefix = prefix + str(i) if prefix[0] == '/' else '/' + prefix + str(i) # "na" -> "/naX"
            for topic, msg, timestamp in tqdm(msg_list, total=len(msg_list), desc="Processing transform & adding prefix ..."):
                if topic == '/tf' and msg.transforms:
                    new_transforms = []
                    for tf_msg in msg.transforms:
                        # 以第一个/tf message的位姿为基准, 将后续message的位姿修改为相对于第一个/tf message的位姿
                        if not keep_start_pose and tf_msg.header.frame_id == frame_pair[0] and tf_msg.child_frame_id == frame_pair[1]:
                            if not initial_tf:
                                initial_tf = copy.deepcopy(tf_msg.transform)
                                tf_msg.transform = calculate_relative_pose(initial_tf, tf_msg.transform)
                            else:
                                final_tf = copy.deepcopy(tf_msg.transform)
                                tf_msg.transform = calculate_relative_pose(initial_tf, tf_msg.transform)
                
                        # For /tf messages, add prefix to each header frame ID and child frame I
                        tf_msg.header.frame_id = current_prefix + tf_msg.header.frame_id if tf_msg.header.frame_id[0] == '/' else current_prefix + '/' + tf_msg.header.frame_id
                        tf_msg.child_frame_id = current_prefix + tf_msg.child_frame_id if tf_msg.child_frame_id[0] == '/' else current_prefix + '/' + tf_msg.child_frame_id

                        new_transforms.append(tf_msg)
                    msg.transforms = new_transforms
                elif msg._has_header:
                    # Add prefix to header frame ID
                    msg.header.frame_id = current_prefix + msg.header.frame_id if msg.header.frame_id[0] == '/' else current_prefix + '/' + msg.header.frame_id
                else:
                    continue
            
                # Add prefix to topic name
                if topic != '/tf':
                    topic = current_prefix + topic if topic[0] == '/' else current_prefix + '/' + topic
                
                # Write the message to the output bagfile
                outbag.write(topic, msg, timestamp)
            
            if print_info:
                print("==========  Segment {} info  ==========".format(i + 1))
                print("Timestamp in original bag: [{:.1f}s, {:.1f}s], duration = {:.1f}s".format(start_time, end_time, end_time - start_time))
                if not manual_cut_file:
                    print("Path in original bag: [{:.3f}m, {:.3f}m], length = {:.3f}m".format(cutting_length_pair_list[i][0], cutting_length_pair_list[i][1], cutting_length_pair_list[i][1] - cutting_length_pair_list[i][0]))
                print("Reversed: " + str(reverse_flag))
                euler, trans = transform_to_euler_and_trans(initial_tf)
                euler = np.degrees(euler)
                print("Initial pose: euler = ({:.3f}, {:.3f}, {:.3f})deg, trans = [{:.3f}, {:.3f}, {:.3f}]m".format(euler[0], euler[1], euler[2], trans[0], trans[1], trans[2]))
                euler, trans = transform_to_euler_and_trans(final_tf)
                euler = np.degrees(euler)
                print("Final pose: euler = ({:.3f}, {:.3f}, {:.3f})deg, trans = [{:.3f}, {:.3f}, {:.3f}]m".format(euler[0], euler[1], euler[2], trans[0], trans[1], trans[2]))
                print("=======================================")
            outbag.close()
                        

           
    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        print('Closing bag files and exit...')
        if inbag: inbag.close() 
        if outbag: outbag.close()

        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""
        输入一个包含单个机器人运动轨迹及机载传感器点云帧的rosbag, 按照指定的切割方式将该rosbag切割为多段, 
        将每一段的初始位姿置为0并随机取正反方向, 然后对每一段的topics添加不同的前缀, 
        最终生成多个新的rosbag文件用于多机器人协同定位与建图算法的测试, 其中每个rosbag文件对应上述流程得到的一段轨迹.
    """)
    parser.add_argument('input_bag_path', help='path of input bag file')
    parser.add_argument('--frame_pair', help='frame pair directly describing the pose of the vehicle in /tf', nargs=2, default=['world', 'imu'])
    parser.add_argument('--num_segment', help='number of segment for auto cutting, default=4', type=int, default=4)
    parser.add_argument('--overlap_ratio', help='overlap ratio for auto cutting, default=0.1(10%)', type=float, default=0.1)
    parser.add_argument('--manual_cut_file', help='path of YAML file specifying manual cutting segments')
    parser.add_argument('--prefix', help="prefix to add to each rosbag, default: 'na' -> add '/naX' before each topic of robot X", default='na')
    parser.add_argument('--keep_start_pose', action='store_true', help='keep start pose for each segment')
    parser.add_argument('--keep_direction', action='store_true', help='keep original direction for each segment')
    parser.add_argument('--print_info', action='store_true', help='print info of each segment')
    
    # rospy.init_node('cut_and_add_prefix', anonymous=True)
    args = parser.parse_args()
    cut_and_add_prefix(
        args.input_bag_path,
        args.frame_pair,
        args.num_segment,
        args.overlap_ratio,
        args.manual_cut_file,
        args.prefix,
        args.keep_start_pose,
        args.keep_direction,
        args.print_info
    ) 
    # rospy.signal_shutdown("Task done.")
