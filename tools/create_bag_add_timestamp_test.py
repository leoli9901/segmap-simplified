import rosbag
import rospy
import time
from std_msgs.msg import Int32, String

bag = rosbag.Bag('test.bag', 'w')

try:
    current_time = time.time()
    print(current_time)
    
    s = String()
    s.data = 'foo'

    i = Int32()
    i.data = 42
    
    s2 = String()
    s2.data = 'bbb'

    bag.write('chatter', s2, rospy.Time.from_sec(current_time + 10.0))
    bag.write('chatter', s, rospy.Time.from_sec(current_time + 5.0))
    bag.write('numbers', i, rospy.Time.from_sec(current_time + 0.0))
finally:
    bag.close()