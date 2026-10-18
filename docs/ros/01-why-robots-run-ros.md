# Why robots run ROS

Nobody picks ROS for rcl or rmw. They pick it for what already runs on top.

Picture a small mobile base asked to cross a cluttered room, pick up a cup, and return. The team does not want to write planners, kinematics, hardware drivers, transforms, logging, and visualization from scratch. They want parts that already talk to each other. That shared set of parts is why robots run ROS.

Navigation comes first for a moving base. The stack at `ros-navigation/navigation2` brings planners, controllers, AMCL, costmaps, BT navigator, and lifecycle bringup. The robot gets a path through the room, a controller that follows it, a localizer that holds its pose on the map, and a bringup sequence that starts those pieces in order.

Manipulation comes next for the arm. The stack at `ros-planning/moveit2` brings planning, IK and FK, execution, servo, and hybrid planning. The same robot reaches for the cup with solved joint angles, monitored execution, and short servo corrections near the grasp.

Control connects those plans to motors. The pair `ros-controls/ros2_control` plus `ros2_controllers` brings hardware interface, joint trajectory, diff drive, and gripper support. The trajectory from the planner becomes wheel and joint commands through a common hardware layer.

Transforms keep every frame straight. The tree at `ros2/geometry2` brings tf2, the frame tree everything looks up. The camera, the gripper, the base, and the map all meet in one lookup instead of scattered math.

Messages carry standard meaning between those parts. The base set at `ros2/common_interfaces` plus domain msgs brings `sensor_msgs`, `geometry_msgs`, `nav_msgs`, and `trajectory_msgs`. A laser scan, a twist, an odometry fix, and a joint trajectory look the same from team to team.

Perception lets the robot see. The set `ros-perception/vision_opencv`, `image_pipeline`, `ros-perception/perception_pcl`, and `ros-drivers` covers lidar, camera, and IMU. Images rectify and debayer, clouds filter and segment, and drivers publish the raw streams those filters consume.

Localization and mapping tells the robot where it is. The pair `cra-ros-pkg/robot_localization` and `SteveMacenski/slam_toolbox` covers state estimation plus mapping. Filters fuse odometry and IMU while the mapper builds and holds the grid the navigator drives on.

Simulation lets the team test before touching hardware. The bridge at `gazebosim/ros_gz`, plus `ros-controls/gazebo_ros2_control`, closes the loop. The same topics flow in simulation and on the robot, so a controller tried in Gazebo behaves the same on the floor.

Visualization and tools show the truth while all this runs. The set `ros2/rviz`, `ros2/ros2cli`, `ros2/rosbag2`, and `ros2/launch_ros` covers display, command line, recording, and launch. The team watches the map and the TF tree, introspects a topic, replays a bag, and starts the whole system from one launch description.

That is the opening picture for the chapters that follow. Each chapter takes one of these working stacks and shows what Weld must carry so it runs unmodified.
