// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package types holds the message corpus fixtures for Phase 1 foundations.
//
// Families:
//   - builtin_interfaces, std_msgs, geometry_msgs, sensor_msgs plus the
//     SetCameraInfo service, trajectory_msgs, nav_msgs with map services,
//     stereo_msgs, shape_msgs, visualization_msgs, diagnostic_msgs, and
//     std_srvs with no message dependencies listed last.
//
// Header:
//   - Header carries stamp plus frame_id. Time carries int32 seconds plus
//     uint32 nanoseconds with Duration alongside.
//   - Odometry header frame_id is the pose parent with twist in
//     child_frame_id.
//   - LaserScan and MultiEchoLaserScan stamps mark first ray acquisition
//     with angles about positive Z from positive X.
//   - Image and DisparityImage stamps mark acquisition in the optical frame
//     with X right plus Y down plus Z forward.
//   - Path header is the path frame with per-pose headers as fallback.
//   - TransformStamped header frame_id is the reference with child_frame_id
//     as target at stamp.
//   - NavSatFix stamp is ROS time in the antenna frame.
//   - Joint stamps are recording times and trajectory stamps are reference
//     times.
//
// Sentinels:
//   - NaN means unmeasured on float fields only, and Inf marks an unbounded
//     range end where the field allows it. Uint8 enum fields carry plain
//     status codes and never carry NaN.
//   - Discard readings outside the sensor range, such as Range outside
//     min_range and max_range or LaserScan outside range_min and range_max.
//   - IMU and MagneticField covariance minus 1 means disregard and all zero
//     means unknown.
//   - Zeroed camera matrices mean uncalibrated.
//   - Signed byte row major occupancy data reads through resolution plus
//     origin.
//
// Order:
//   - Entries appear in topological generation order: builtin_interfaces,
//     then std_msgs, then geometry_msgs, then sensor_msgs plus
//     trajectory_msgs plus nav_msgs (tiers 3a, 3b, 3c), then stereo_msgs
//     plus shape_msgs plus visualization_msgs plus diagnostic_msgs (tiers
//     4a, 4b, 4c, 4d). std_srvs has no message dependencies and is listed
//     last. Point cloud Python helpers are utility, not codegen target.
//   - Dependencies lists direct "package/name" requirements in field order
//     with std_msgs/Header first when present, and every dependency appears
//     earlier in the table so the DAG resolves in order.
//
// Codegen:
//   - Summary is a human-only field shape sketch, not a machine parseable
//     schema and not a codegen input. Codegen stays gated on the schema
//     registry plus the external ROS IDL, not on this corpus.
//   - Structured Fields, Bounds, and Constants remain deferred until the
//     registry path needs them; the corpus keeps full field lists without
//     etc placeholders so the sketch stays reviewable.
//
// Sync:
//   - The table is synced by hand from ROS IDL at
//     https://github.com/ros2/common_interfaces/tree/jazzy for the Jazzy
//     distro. To sync, diff each Summary against the linked IDL, keep
//     topological order and Header-first field order, then run
//     go test ./internal/types/ -run TestCorpus -count=1.
//
// OPEN ITEM: bounded strings have no corpus fixture, so bounded string
// support stays gated as unproven until an external fixture proves it.
package types

// CorpusKind names the IDL definition kind for a corpus entry.
type CorpusKind string

// Corpus definition kinds. Phase 1 covers messages plus services only;
// actions are out of scope.
const (
	// KindMessage is one struct definition.
	KindMessage CorpusKind = "message"
	// KindService is request plus response plus event definitions.
	KindService CorpusKind = "service"
)

// CorpusEntry is one fixture row in the message corpus.
type CorpusEntry struct {
	// Package is the ROS package name such as std_msgs.
	Package string
	// Name is the message or service base name such as String.
	Name string
	// Kind is message or service.
	Kind CorpusKind
	// Summary is a short human-only field shape sketch, not a machine
	// parseable schema. Sentinel, enum, and lifetime hints may trail the shape.
	Summary string
	// HeaderNote records header-only semantics for types where stamp and frame
	// handling matters. Empty when the type has no header rule.
	HeaderNote string
	// Dependencies lists direct "package/name" requirements in field order
	// with std_msgs/Header first when present. Every dependency appears
	// earlier in the table than its users.
	Dependencies []string
	// HasHeader reports a direct std_msgs/Header field on the type.
	// std_msgs/Header itself is false: it defines the header.
	HasHeader bool
	// TypeHash reserves the RIHS01 hash string. Empty in Phase 1 fixtures.
	TypeHash string
}

// corpus is the fixture table in topological generation order.
var corpus = []CorpusEntry{
	// Tier 0: builtin_interfaces.
	{Package: "builtin_interfaces", Name: "Duration", Kind: KindMessage, Summary: "int32 sec, uint32 nanosec"},
	{Package: "builtin_interfaces", Name: "Time", Kind: KindMessage, Summary: "int32 sec, uint32 nanosec"},

	// Tier 1: std_msgs primitives plus Header, then layout, then arrays.
	{Package: "std_msgs", Name: "Bool", Kind: KindMessage, Summary: "bool data"},
	{Package: "std_msgs", Name: "Byte", Kind: KindMessage, Summary: "byte data"},
	{Package: "std_msgs", Name: "Char", Kind: KindMessage, Summary: "char data"},
	{Package: "std_msgs", Name: "ColorRGBA", Kind: KindMessage, Summary: "float32 r, g, b, a"},
	{Package: "std_msgs", Name: "Empty", Kind: KindMessage, Summary: "no fields"},
	{Package: "std_msgs", Name: "Float32", Kind: KindMessage, Summary: "float32 data"},
	{Package: "std_msgs", Name: "Float64", Kind: KindMessage, Summary: "float64 data"},
	{Package: "std_msgs", Name: "Header", Kind: KindMessage, Summary: "Time stamp, string frame_id", HeaderNote: "Header carries stamp plus frame_id", Dependencies: []string{"builtin_interfaces/Time"}},
	{Package: "std_msgs", Name: "Int16", Kind: KindMessage, Summary: "int16 data"},
	{Package: "std_msgs", Name: "Int32", Kind: KindMessage, Summary: "int32 data"},
	{Package: "std_msgs", Name: "Int64", Kind: KindMessage, Summary: "int64 data"},
	{Package: "std_msgs", Name: "Int8", Kind: KindMessage, Summary: "int8 data"},
	{Package: "std_msgs", Name: "String", Kind: KindMessage, Summary: "string data"},
	{Package: "std_msgs", Name: "UInt16", Kind: KindMessage, Summary: "uint16 data"},
	{Package: "std_msgs", Name: "UInt32", Kind: KindMessage, Summary: "uint32 data"},
	{Package: "std_msgs", Name: "UInt64", Kind: KindMessage, Summary: "uint64 data"},
	{Package: "std_msgs", Name: "UInt8", Kind: KindMessage, Summary: "uint8 data"},
	{Package: "std_msgs", Name: "MultiArrayDimension", Kind: KindMessage, Summary: "string label, uint32 size, stride"},
	{Package: "std_msgs", Name: "MultiArrayLayout", Kind: KindMessage, Summary: "MultiArrayDimension[] dim, uint32 data_offset", Dependencies: []string{"std_msgs/MultiArrayDimension"}},
	{Package: "std_msgs", Name: "ByteMultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, byte[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Float32MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, float32[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Float64MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, float64[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Int16MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, int16[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Int32MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, int32[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Int64MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, int64[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "Int8MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, int8[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "UInt16MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, uint16[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "UInt32MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, uint32[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "UInt64MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, uint64[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},
	{Package: "std_msgs", Name: "UInt8MultiArray", Kind: KindMessage, Summary: "MultiArrayLayout layout, uint8[] data", Dependencies: []string{"std_msgs/MultiArrayLayout"}},

	// Tier 2: geometry_msgs in dependency order.
	{Package: "geometry_msgs", Name: "Point", Kind: KindMessage, Summary: "float64 x, y, z"},
	{Package: "geometry_msgs", Name: "Point32", Kind: KindMessage, Summary: "float32 x, y, z"},
	{Package: "geometry_msgs", Name: "Quaternion", Kind: KindMessage, Summary: "float64 x, y, z, w identity default"},
	{Package: "geometry_msgs", Name: "Vector3", Kind: KindMessage, Summary: "float64 x, y, z"},
	{Package: "geometry_msgs", Name: "Accel", Kind: KindMessage, Summary: "Vector3 linear, Vector3 angular", Dependencies: []string{"geometry_msgs/Vector3"}},
	{Package: "geometry_msgs", Name: "Inertia", Kind: KindMessage, Summary: "float64 m, Vector3 com, float64 ixx, ixy, ixz, iyy, iyz, izz", Dependencies: []string{"geometry_msgs/Vector3"}},
	{Package: "geometry_msgs", Name: "Polygon", Kind: KindMessage, Summary: "Point32[] points", Dependencies: []string{"geometry_msgs/Point32"}},
	{Package: "geometry_msgs", Name: "Pose", Kind: KindMessage, Summary: "Point position, Quaternion orientation identity default", Dependencies: []string{"geometry_msgs/Point", "geometry_msgs/Quaternion"}},
	{Package: "geometry_msgs", Name: "Transform", Kind: KindMessage, Summary: "Vector3 translation, Quaternion rotation", Dependencies: []string{"geometry_msgs/Vector3", "geometry_msgs/Quaternion"}},
	{Package: "geometry_msgs", Name: "Twist", Kind: KindMessage, Summary: "Vector3 linear, Vector3 angular", Dependencies: []string{"geometry_msgs/Vector3"}},
	{Package: "geometry_msgs", Name: "Wrench", Kind: KindMessage, Summary: "Vector3 force, Vector3 torque", Dependencies: []string{"geometry_msgs/Vector3"}},
	{Package: "geometry_msgs", Name: "AccelWithCovariance", Kind: KindMessage, Summary: "Accel accel, float64[36] covariance inline", Dependencies: []string{"geometry_msgs/Accel"}},
	{Package: "geometry_msgs", Name: "PoseWithCovariance", Kind: KindMessage, Summary: "Pose pose, float64[36] covariance inline", Dependencies: []string{"geometry_msgs/Pose"}},
	{Package: "geometry_msgs", Name: "TwistWithCovariance", Kind: KindMessage, Summary: "Twist twist, float64[36] covariance inline", Dependencies: []string{"geometry_msgs/Twist"}},
	{Package: "geometry_msgs", Name: "AccelStamped", Kind: KindMessage, Summary: "Header header, Accel accel", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Accel"}},
	{Package: "geometry_msgs", Name: "AccelWithCovarianceStamped", Kind: KindMessage, Summary: "Header header, AccelWithCovariance accel", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/AccelWithCovariance"}},
	{Package: "geometry_msgs", Name: "InertiaStamped", Kind: KindMessage, Summary: "Header header, Inertia inertia", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Inertia"}},
	{Package: "geometry_msgs", Name: "PointStamped", Kind: KindMessage, Summary: "Header header, Point point", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Point"}},
	{Package: "geometry_msgs", Name: "PolygonStamped", Kind: KindMessage, Summary: "Header header, Polygon polygon", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Polygon"}},
	{Package: "geometry_msgs", Name: "PoseArray", Kind: KindMessage, Summary: "Header header, Pose[] poses", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose"}},
	{Package: "geometry_msgs", Name: "PoseStamped", Kind: KindMessage, Summary: "Header header, Pose pose", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose"}},
	{Package: "geometry_msgs", Name: "PoseWithCovarianceStamped", Kind: KindMessage, Summary: "Header header, PoseWithCovariance pose", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/PoseWithCovariance"}},
	{Package: "geometry_msgs", Name: "QuaternionStamped", Kind: KindMessage, Summary: "Header header, Quaternion quaternion", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Quaternion"}},
	{Package: "geometry_msgs", Name: "TransformStamped", Kind: KindMessage, Summary: "Header header, string child_frame_id, Transform transform", HeaderNote: "Header frame_id is reference, child_frame_id is target at stamp", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Transform"}},
	{Package: "geometry_msgs", Name: "TwistStamped", Kind: KindMessage, Summary: "Header header, Twist twist", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Twist"}},
	{Package: "geometry_msgs", Name: "TwistWithCovarianceStamped", Kind: KindMessage, Summary: "Header header, TwistWithCovariance twist", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/TwistWithCovariance"}},
	{Package: "geometry_msgs", Name: "Vector3Stamped", Kind: KindMessage, Summary: "Header header, Vector3 vector", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Vector3"}},
	{Package: "geometry_msgs", Name: "WrenchStamped", Kind: KindMessage, Summary: "Header header, Wrench wrench", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Wrench"}},

	// Tier 3a: sensor_msgs in dependency order.
	{Package: "sensor_msgs", Name: "BatteryState", Kind: KindMessage, Summary: "float32 voltage, temperature, current, charge, capacity, design_capacity, percentage, uint8 supply/health/technology enums, float32[] cells, string location; float NaN when unknown", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "ChannelFloat32", Kind: KindMessage, Summary: "string name, float32[] values"},
	{Package: "sensor_msgs", Name: "CompressedImage", Kind: KindMessage, Summary: "Header header, string format, uint8[] data", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "FluidPressure", Kind: KindMessage, Summary: "Header header, float64 fluid_pressure, variance; NaN means unmeasured", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "Illuminance", Kind: KindMessage, Summary: "Header header, float64 illuminance, variance; NaN means unmeasured", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "Image", Kind: KindMessage, Summary: "Header header, uint32 height, width, string encoding, uint8 is_bigendian, uint32 step, uint8[] data", HeaderNote: "Image stamp is acquisition in optical frame X right Y down Z forward", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "Imu", Kind: KindMessage, Summary: "Header header, Quaternion orientation, float64[9] orientation_covariance inline, Vector3 angular_velocity, float64[9] angular_velocity_covariance inline, Vector3 linear_acceleration, float64[9] linear_acceleration_covariance inline; covariance -1 means disregard, all zero means unknown", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Quaternion", "geometry_msgs/Vector3"}},
	{Package: "sensor_msgs", Name: "JointState", Kind: KindMessage, Summary: "Header header, string[] name, float64[] position, velocity, effort", HeaderNote: "Joint stamp is recording time", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "Joy", Kind: KindMessage, Summary: "Header header, float32[] axes, int32[] buttons", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "JoyFeedback", Kind: KindMessage, Summary: "uint8 type, id, float32 intensity; uint8 enums"},
	{Package: "sensor_msgs", Name: "JoyFeedbackArray", Kind: KindMessage, Summary: "JoyFeedback[] array", Dependencies: []string{"sensor_msgs/JoyFeedback"}},
	{Package: "sensor_msgs", Name: "LaserEcho", Kind: KindMessage, Summary: "float32[] echoes"},
	{Package: "sensor_msgs", Name: "LaserScan", Kind: KindMessage, Summary: "Header header, float32 angle_min, angle_max, angle_increment, time_increment, scan_time, range_min, range_max, float32[] ranges, intensities; discard outside range_min/range_max", HeaderNote: "LaserScan stamp is first ray acquisition; angles sweep about +Z from +X", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "MagneticField", Kind: KindMessage, Summary: "Header header, Vector3 magnetic_field, float64[9] covariance inline; covariance -1 means disregard", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Vector3"}},
	{Package: "sensor_msgs", Name: "MultiDOFJointState", Kind: KindMessage, Summary: "Header header, string[] joint_names, Transform[] transforms, Twist[] twists, Wrench[] wrenches", HeaderNote: "Joint stamp is recording time", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Transform", "geometry_msgs/Twist", "geometry_msgs/Wrench"}},
	{Package: "sensor_msgs", Name: "MultiEchoLaserScan", Kind: KindMessage, Summary: "Header header, float32 angle_min, angle_max, angle_increment, time_increment, scan_time, range_min, range_max, LaserEcho[] ranges, intensities; discard outside range_min/range_max", HeaderNote: "MultiEchoLaserScan stamp is first ray acquisition; angles sweep about +Z from +X", HasHeader: true, Dependencies: []string{"std_msgs/Header", "sensor_msgs/LaserEcho"}},
	{Package: "sensor_msgs", Name: "NavSatStatus", Kind: KindMessage, Summary: "int8 status, uint16 service; uint8 enums"},
	{Package: "sensor_msgs", Name: "NavSatFix", Kind: KindMessage, Summary: "Header header, NavSatStatus status, float64 latitude, longitude, altitude, float64[9] position_covariance inline, uint8 position_covariance_type", HeaderNote: "NavSatFix stamp is ROS time in antenna frame", HasHeader: true, Dependencies: []string{"std_msgs/Header", "sensor_msgs/NavSatStatus"}},
	{Package: "sensor_msgs", Name: "PointCloud", Kind: KindMessage, Summary: "Header header, Point32[] points, ChannelFloat32[] channels", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Point32", "sensor_msgs/ChannelFloat32"}},
	{Package: "sensor_msgs", Name: "PointField", Kind: KindMessage, Summary: "string name, uint32 offset, uint8 datatype, count; uint8 enums"},
	{Package: "sensor_msgs", Name: "PointCloud2", Kind: KindMessage, Summary: "Header header, uint32 height, width, PointField[] fields, bool is_bigendian, uint32 point_step, row_step, uint8[] data, bool is_dense", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "sensor_msgs/PointField"}},
	{Package: "sensor_msgs", Name: "Range", Kind: KindMessage, Summary: "Header header, uint8 radiation_type enum, float32 field_of_view, min_range, max_range, range; discard outside min_range/max_range", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "RegionOfInterest", Kind: KindMessage, Summary: "uint32 x_offset, y_offset, height, width, bool do_rectify"},
	{Package: "sensor_msgs", Name: "CameraInfo", Kind: KindMessage, Summary: "Header header, uint32 height, width, string distortion_model, float64[] d, float64[9] k inline, float64[9] r inline, float64[12] p inline, uint32 binning, RegionOfInterest roi; zeroed k r p mean uncalibrated", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "sensor_msgs/RegionOfInterest"}},
	{Package: "sensor_msgs", Name: "RelativeHumidity", Kind: KindMessage, Summary: "Header header, float64 relative_humidity, variance; NaN means unmeasured", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "Temperature", Kind: KindMessage, Summary: "Header header, float64 temperature, variance; NaN means unmeasured", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header"}},
	{Package: "sensor_msgs", Name: "TimeReference", Kind: KindMessage, Summary: "Header header, Time time_ref, string source; time_ref holds the source time", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "builtin_interfaces/Time"}},
	{Package: "sensor_msgs", Name: "SetCameraInfo", Kind: KindService, Summary: "request CameraInfo camera_info, response bool success, string status_message", Dependencies: []string{"sensor_msgs/CameraInfo"}},

	// Tier 3b: trajectory_msgs in dependency order.
	{Package: "trajectory_msgs", Name: "JointTrajectoryPoint", Kind: KindMessage, Summary: "float64[] positions, velocities, accelerations, effort, Duration time_from_start", Dependencies: []string{"builtin_interfaces/Duration"}},
	{Package: "trajectory_msgs", Name: "JointTrajectory", Kind: KindMessage, Summary: "Header header, string[] joint_names, JointTrajectoryPoint[] points", HeaderNote: "Trajectory stamp is reference time", HasHeader: true, Dependencies: []string{"std_msgs/Header", "trajectory_msgs/JointTrajectoryPoint"}},
	{Package: "trajectory_msgs", Name: "MultiDOFJointTrajectoryPoint", Kind: KindMessage, Summary: "Transform[] transforms, Twist[] velocities, accelerations, Duration time_from_start", Dependencies: []string{"geometry_msgs/Transform", "geometry_msgs/Twist", "builtin_interfaces/Duration"}},
	{Package: "trajectory_msgs", Name: "MultiDOFJointTrajectory", Kind: KindMessage, Summary: "Header header, string[] joint_names, MultiDOFJointTrajectoryPoint[] points", HeaderNote: "Trajectory stamp is reference time", HasHeader: true, Dependencies: []string{"std_msgs/Header", "trajectory_msgs/MultiDOFJointTrajectoryPoint"}},

	// Tier 3c: nav_msgs.
	{Package: "nav_msgs", Name: "GridCells", Kind: KindMessage, Summary: "Header header, float32 cell_width, cell_height, Point[] cells", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Point"}},
	{Package: "nav_msgs", Name: "MapMetaData", Kind: KindMessage, Summary: "Time map_load_time, float32 resolution, uint32 width, height, Pose origin", Dependencies: []string{"builtin_interfaces/Time", "geometry_msgs/Pose"}},
	{Package: "nav_msgs", Name: "OccupancyGrid", Kind: KindMessage, Summary: "Header header, MapMetaData info, int8[] data row major; signed byte row major occupancy via resolution plus origin", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "nav_msgs/MapMetaData"}},
	{Package: "nav_msgs", Name: "Odometry", Kind: KindMessage, Summary: "Header header, string child_frame_id, PoseWithCovariance pose, TwistWithCovariance twist", HeaderNote: "Odometry header frame_id is pose parent with twist in child_frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/PoseWithCovariance", "geometry_msgs/TwistWithCovariance"}},
	{Package: "nav_msgs", Name: "Path", Kind: KindMessage, Summary: "Header header, PoseStamped[] poses", HeaderNote: "Path header is path frame with per-pose headers as fallback", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/PoseStamped"}},
	{Package: "nav_msgs", Name: "GetMap", Kind: KindService, Summary: "request empty, response OccupancyGrid map", Dependencies: []string{"nav_msgs/OccupancyGrid"}},
	{Package: "nav_msgs", Name: "GetPlan", Kind: KindService, Summary: "request PoseStamped start, goal, float32 tolerance, response Path plan", Dependencies: []string{"geometry_msgs/PoseStamped", "nav_msgs/Path"}},
	{Package: "nav_msgs", Name: "LoadMap", Kind: KindService, Summary: "request string map_url, response OccupancyGrid map, uint8 result", Dependencies: []string{"nav_msgs/OccupancyGrid"}},
	{Package: "nav_msgs", Name: "SetMap", Kind: KindService, Summary: "request OccupancyGrid map, PoseWithCovarianceStamped initial_pose, response bool success", Dependencies: []string{"nav_msgs/OccupancyGrid", "geometry_msgs/PoseWithCovarianceStamped"}},

	// Tier 4a: stereo_msgs.
	{Package: "stereo_msgs", Name: "DisparityImage", Kind: KindMessage, Summary: "Header header, Image image, float32 f, t, RegionOfInterest valid_window, float32 min_disparity, max_disparity, delta_d", HeaderNote: "DisparityImage stamp is left image acquisition in optical frame X right Y down Z forward", HasHeader: true, Dependencies: []string{"std_msgs/Header", "sensor_msgs/Image", "sensor_msgs/RegionOfInterest"}},

	// Tier 4b: shape_msgs in dependency order.
	{Package: "shape_msgs", Name: "MeshTriangle", Kind: KindMessage, Summary: "uint32[3] vertex_indices inline"},
	{Package: "shape_msgs", Name: "Mesh", Kind: KindMessage, Summary: "MeshTriangle[] triangles, Point[] vertices", Dependencies: []string{"shape_msgs/MeshTriangle", "geometry_msgs/Point"}},
	{Package: "shape_msgs", Name: "Plane", Kind: KindMessage, Summary: "float64[4] coef inline"},
	{Package: "shape_msgs", Name: "SolidPrimitive", Kind: KindMessage, Summary: "uint8 type BOX SPHERE CYLINDER CONE PRISM enums, float64[] dimensions bounded max 3"},

	// Tier 4c: visualization_msgs in dependency order.
	{Package: "visualization_msgs", Name: "ImageMarker", Kind: KindMessage, Summary: "Header header, string ns, int32 id, int32 type CIRCLE LINE_STRIP LINE_LIST POLYGON POINTS enums, int32 action ADD REMOVE enums, Point position, float32 scale, ColorRGBA outline_color, uint8 filled, ColorRGBA fill_color, Duration lifetime zero means forever, Point[] points, ColorRGBA[] outline_colors", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Point", "std_msgs/ColorRGBA", "builtin_interfaces/Duration"}},
	{Package: "visualization_msgs", Name: "Marker", Kind: KindMessage, Summary: "Header header, string ns, int32 id, int32 type ARROW CUBE SPHERE CYLINDER LINE_STRIP LINE_LIST CUBE_LIST SPHERE_LIST POINTS TEXT_VIEW_FACING MESH_RESOURCE TRIANGLE_LIST enums, int32 action ADD DELETE DELETEALL enums, Pose pose, Vector3 scale, ColorRGBA color, Duration lifetime zero means forever, points, colors, text", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose", "geometry_msgs/Vector3", "std_msgs/ColorRGBA", "builtin_interfaces/Duration", "geometry_msgs/Point"}},
	{Package: "visualization_msgs", Name: "MarkerArray", Kind: KindMessage, Summary: "Marker[] markers; per-element lifetime zero means forever", Dependencies: []string{"visualization_msgs/Marker"}},
	{Package: "visualization_msgs", Name: "MenuEntry", Kind: KindMessage, Summary: "uint32 id, parent_id, string title, command, command_type"},
	{Package: "visualization_msgs", Name: "InteractiveMarkerControl", Kind: KindMessage, Summary: "string name, Quaternion orientation, uint8 orientation_mode INHERIT FIXED VIEW_FACING enums, uint8 interaction_mode NONE MENU BUTTON MOVE_AXIS MOVE_PLANE ROTATE_AXIS MOVE_ROTATE MOVE_3D ROTATE_3D MOVE_ROTATE_3D enums, bool always_visible, Marker[] markers, bool independent_marker_orientation, string description", Dependencies: []string{"geometry_msgs/Quaternion", "visualization_msgs/Marker"}},
	{Package: "visualization_msgs", Name: "InteractiveMarker", Kind: KindMessage, Summary: "Header header, string name, description, Pose pose, Vector3 scale, InteractiveMarkerControl[] controls", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose", "geometry_msgs/Vector3", "visualization_msgs/InteractiveMarkerControl"}},
	{Package: "visualization_msgs", Name: "InteractiveMarkerInit", Kind: KindMessage, Summary: "string server_id, uint64 seq_num, InteractiveMarker[] markers", Dependencies: []string{"visualization_msgs/InteractiveMarker"}},
	{Package: "visualization_msgs", Name: "InteractiveMarkerPose", Kind: KindMessage, Summary: "Header header, string name, Pose pose", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose"}},
	{Package: "visualization_msgs", Name: "InteractiveMarkerFeedback", Kind: KindMessage, Summary: "Header header, string client_id, marker_name, control_name, uint8 event_type KEEP_ALIVE POSE_UPDATE MENU_SELECT BUTTON_CLICK MOUSE_DOWN MOUSE_UP enums, Pose pose, uint32 menu_entry_id, Point mouse_point, bool mouse_point_valid", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "geometry_msgs/Pose", "geometry_msgs/Point"}},
	{Package: "visualization_msgs", Name: "InteractiveMarkerUpdate", Kind: KindMessage, Summary: "string server_id, uint64 seq_num, uint8 type KEEP_ALIVE UPDATE enums, InteractiveMarker[] markers, InteractiveMarkerPose[] poses, string[] erases", Dependencies: []string{"visualization_msgs/InteractiveMarker", "visualization_msgs/InteractiveMarkerPose"}},

	// Tier 4d: diagnostic_msgs in dependency order.
	{Package: "diagnostic_msgs", Name: "KeyValue", Kind: KindMessage, Summary: "string key, value"},
	{Package: "diagnostic_msgs", Name: "DiagnosticStatus", Kind: KindMessage, Summary: "byte level OK WARN ERROR STALE enums, string name, message, hardware_id, KeyValue[] values", Dependencies: []string{"diagnostic_msgs/KeyValue"}},
	{Package: "diagnostic_msgs", Name: "DiagnosticArray", Kind: KindMessage, Summary: "Header header, DiagnosticStatus[] status", HeaderNote: "Header carries stamp plus frame_id", HasHeader: true, Dependencies: []string{"std_msgs/Header", "diagnostic_msgs/DiagnosticStatus"}},
	{Package: "diagnostic_msgs", Name: "AddDiagnostics", Kind: KindService, Summary: "request string load_namespace, response bool success, string message"},
	{Package: "diagnostic_msgs", Name: "SelfTest", Kind: KindService, Summary: "request empty, response string id, bool passed, DiagnosticStatus[] status", Dependencies: []string{"diagnostic_msgs/DiagnosticStatus"}},

	// No message dependencies: std_srvs listed last.
	{Package: "std_srvs", Name: "Empty", Kind: KindService, Summary: "request empty, response empty"},
	{Package: "std_srvs", Name: "SetBool", Kind: KindService, Summary: "request bool data, response bool success, string message"},
	{Package: "std_srvs", Name: "Trigger", Kind: KindService, Summary: "request empty, response bool success, string message"},
}

// Corpus returns the fixture table in topological generation order.
//
// The returned slice is a fresh copy in stable order; mutating the entries
// does not affect the table. Dependencies hold direct "package/name"
// requirements in field order with std_msgs/Header first when present, and
// every dependency appears earlier in the table.
func Corpus() []CorpusEntry {
	out := make([]CorpusEntry, len(corpus))
	for i, e := range corpus {
		out[i] = e
		if e.Dependencies != nil {
			deps := make([]string, len(e.Dependencies))
			copy(deps, e.Dependencies)
			out[i].Dependencies = deps
		}
	}
	return out
}

// Lookup finds one corpus entry by package and base name.
// Matching is exact and case-sensitive with O(n) scan cost. It reports
// false when the pair is not in the fixture table. The returned entry owns
// its Dependencies slice so mutating it does not affect the table.
func Lookup(pkg, name string) (CorpusEntry, bool) {
	for _, e := range corpus {
		if e.Package == pkg && e.Name == name {
			if e.Dependencies != nil {
				deps := make([]string, len(e.Dependencies))
				copy(deps, e.Dependencies)
				e.Dependencies = deps
			}
			return e, true
		}
	}
	return CorpusEntry{}, false
}
