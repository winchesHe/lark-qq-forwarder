// swift-tools-version: 5.10

import PackageDescription

let package = Package(
  name: "LarkNotificationProbe",
  platforms: [
    .macOS(.v13)
  ],
  products: [
    .executable(
      name: "lark-notification-probe",
      targets: ["LarkNotificationProbe"]
    ),
    .library(
      name: "LarkNotificationProbeCore",
      targets: ["LarkNotificationProbeCore"]
    ),
  ],
  targets: [
    .target(name: "LarkNotificationProbeCore"),
    .executableTarget(
      name: "LarkNotificationProbe",
      dependencies: ["LarkNotificationProbeCore"]
    ),
    .testTarget(
      name: "LarkNotificationProbeCoreTests",
      dependencies: ["LarkNotificationProbeCore"]
    ),
  ]
)
