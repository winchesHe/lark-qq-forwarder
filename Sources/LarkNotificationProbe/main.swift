import AppKit
import ApplicationServices
import Foundation
import LarkNotificationProbeCore

private enum ProbeError: Error, CustomStringConvertible {
  case invalidArgument(String)
  case accessibilityPermissionMissing
  case notificationCenterNotRunning
  case outputOpenFailed(String)

  var description: String {
    switch self {
    case .invalidArgument(let message):
      return message
    case .accessibilityPermissionMissing:
      return "缺少辅助功能权限。请在“系统设置 → 隐私与安全性 → 辅助功能”中允许启动本程序的终端，然后重新运行。"
    case .notificationCenterNotRunning:
      return "没有找到 macOS 通知中心进程。"
    case .outputOpenFailed(let path):
      return "无法打开输出文件：\(path)"
    }
  }
}

private struct Configuration {
  var outputPath = FileManager.default.currentDirectoryPath + "/lark-notifications.jsonl"
  var allowedApps = NotificationParser.defaultAllowedApps
  var includeExisting = false
  var promptPermission = false
  var debug = false
  var checkOnly = false
  var duration: TimeInterval?

  static func parse(arguments: [String]) throws -> Configuration {
    var configuration = Configuration()
    var index = 0

    while index < arguments.count {
      let argument = arguments[index]
      switch argument {
      case "--output":
        index += 1
        guard index < arguments.count else {
          throw ProbeError.invalidArgument("--output 后缺少文件路径")
        }
        configuration.outputPath = NSString(string: arguments[index]).expandingTildeInPath
      case "--app-name":
        index += 1
        guard index < arguments.count else {
          throw ProbeError.invalidArgument("--app-name 后缺少应用名")
        }
        configuration.allowedApps.insert(arguments[index].lowercased())
      case "--duration":
        index += 1
        guard index < arguments.count,
          let duration = TimeInterval(arguments[index]),
          duration > 0
        else {
          throw ProbeError.invalidArgument("--duration 必须是大于 0 的秒数")
        }
        configuration.duration = duration
      case "--include-existing":
        configuration.includeExisting = true
      case "--prompt-permission":
        configuration.promptPermission = true
      case "--debug":
        configuration.debug = true
      case "--check":
        configuration.checkOnly = true
      case "--help", "-h":
        printHelp()
        exit(0)
      default:
        throw ProbeError.invalidArgument("未知参数：\(argument)")
      }
      index += 1
    }

    return configuration
  }

  private static func printHelp() {
    print(
      """
      飞书本地通知验证器

      用法：
        lark-notification-probe [选项]

      选项：
        --output <路径>          JSONL 输出文件
        --duration <秒>          到时自动退出
        --include-existing       启动时也输出当前已有通知
        --prompt-permission      请求 macOS 辅助功能权限
        --app-name <名称>        增加允许的飞书显示名称
        --check                  只检查环境和权限
        --debug                  输出诊断信息，不输出其他应用正文
        --help                   显示帮助
      """
    )
  }
}

private final class JSONLWriter {
  private let handle: FileHandle
  let path: String

  init(path: String) throws {
    self.path = path
    let url = URL(fileURLWithPath: path)
    let parent = url.deletingLastPathComponent()

    do {
      try FileManager.default.createDirectory(
        at: parent,
        withIntermediateDirectories: true
      )
      if !FileManager.default.fileExists(atPath: path) {
        guard FileManager.default.createFile(atPath: path, contents: nil) else {
          throw ProbeError.outputOpenFailed(path)
        }
      }
      handle = try FileHandle(forWritingTo: url)
      try handle.seekToEnd()
    } catch {
      throw ProbeError.outputOpenFailed(path)
    }
  }

  func append(_ notification: ParsedNotification) {
    let payload: [String: Any] = [
      "schema_version": 1,
      "type": "notification",
      "source": "macos_accessibility",
      "observed_at": Self.timestamp(),
      "bundle_id": "com.electron.lark",
      "app": notification.app,
      "title": notification.title,
      "body": notification.body,
      "subtitle": notification.subtitle,
      "raw_texts": notification.rawTexts,
      "fingerprint": notification.fingerprint,
    ]

    guard JSONSerialization.isValidJSONObject(payload),
      let data = try? JSONSerialization.data(
        withJSONObject: payload,
        options: [.sortedKeys]
      )
    else {
      return
    }

    handle.write(data)
    handle.write(Data([0x0A]))
    try? handle.synchronize()
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data([0x0A]))
  }

  private static func timestamp() -> String {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.string(from: Date())
  }
}

private final class NotificationProbe {
  private let configuration: Configuration
  private let writer: JSONLWriter
  private var observers: [pid_t: AXObserver] = [:]
  private var activeNotificationKeys = Set<String>()
  private var lastDeliveredAt: [String: Date] = [:]
  private var isPrimed = false
  private var scanScheduled = false

  private let notificationBundleIDs: Set<String> = [
    "com.apple.notificationcenterui",
    "com.apple.UserNotificationCenter",
  ]
  private let notificationProcessNames = [
    "NotificationCenter",
    "UserNotificationCenter",
  ]
  private let observedNotifications: [CFString] = [
    kAXWindowCreatedNotification as CFString,
    kAXMainWindowChangedNotification as CFString,
    kAXFocusedWindowChangedNotification as CFString,
    kAXFocusedUIElementChangedNotification as CFString,
  ]

  init(configuration: Configuration) throws {
    self.configuration = configuration
    writer = try JSONLWriter(path: configuration.outputPath)
  }

  func run() throws {
    guard checkAccessibility(prompt: configuration.promptPermission) else {
      throw ProbeError.accessibilityPermissionMissing
    }

    let apps = notificationCenterApplications()
    guard !apps.isEmpty else {
      throw ProbeError.notificationCenterNotRunning
    }

    registerObservers(for: apps)
    scan()

    Timer.scheduledTimer(withTimeInterval: 0.35, repeats: true) { [weak self] _ in
      self?.scan()
    }

    if let duration = configuration.duration {
      Timer.scheduledTimer(withTimeInterval: duration, repeats: false) { _ in
        exit(0)
      }
    }

    log(
      "已开始监听；输出：\(writer.path)；辅助功能观察器：\(observers.count) 个",
      always: true
    )
    RunLoop.current.run()
  }

  private func checkAccessibility(prompt: Bool) -> Bool {
    AXIsProcessTrustedWithOptions(
      [
        "AXTrustedCheckOptionPrompt": prompt
      ] as CFDictionary)
  }

  private func notificationCenterApplications() -> [NSRunningApplication] {
    NSWorkspace.shared.runningApplications.filter { app in
      if let bundleIdentifier = app.bundleIdentifier,
        notificationBundleIDs.contains(bundleIdentifier)
      {
        return true
      }

      let name = app.localizedName ?? ""
      return notificationProcessNames.contains { name.contains($0) }
    }
  }

  private func registerObservers(for apps: [NSRunningApplication]) {
    let callback: AXObserverCallback = { _, _, _, pointer in
      guard let pointer else {
        return
      }
      let probe = Unmanaged<NotificationProbe>
        .fromOpaque(pointer)
        .takeUnretainedValue()
      probe.scheduleScan()
    }
    let pointer = UnsafeMutableRawPointer(Unmanaged.passUnretained(self).toOpaque())

    for app in apps where observers[app.processIdentifier] == nil {
      let pid = app.processIdentifier
      let appElement = AXUIElementCreateApplication(pid)
      var observer: AXObserver?

      guard AXObserverCreate(pid, callback, &observer) == .success,
        let observer
      else {
        log("无法为 pid=\(pid) 创建观察器")
        continue
      }

      var registrationCount = 0
      for notification in observedNotifications {
        let result = AXObserverAddNotification(
          observer,
          appElement,
          notification,
          pointer
        )
        if result == .success || result == .notificationAlreadyRegistered {
          registrationCount += 1
        }
      }

      guard registrationCount > 0 else {
        log("pid=\(pid) 不支持目标辅助功能事件，将由定时扫描兜底")
        continue
      }

      observers[pid] = observer
      CFRunLoopAddSource(
        CFRunLoopGetCurrent(),
        AXObserverGetRunLoopSource(observer),
        .defaultMode
      )
    }
  }

  private func scheduleScan() {
    guard !scanScheduled else {
      return
    }
    scanScheduled = true
    DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) { [weak self] in
      self?.scanScheduled = false
      self?.scan()
    }
  }

  private func scan() {
    let apps = notificationCenterApplications()
    registerObservers(for: apps)

    var capturedByKey: [String: ParsedNotification] = [:]
    for app in apps {
      let appElement = AXUIElementCreateApplication(app.processIdentifier)
      var candidates: [AXUIElement] = []
      let windows: [AXUIElement] =
        attribute(
          appElement,
          kAXWindowsAttribute
        ) ?? []
      let children: [AXUIElement] =
        attribute(
          appElement,
          kAXChildrenAttribute
        ) ?? []
      candidates.append(contentsOf: windows)
      candidates.append(contentsOf: children)

      for candidate in candidates {
        var visited = Set<UInt>()
        let alerts = findAlertElements(
          in: candidate,
          depth: 0,
          visited: &visited
        )

        if alerts.isEmpty {
          capture(candidate, into: &capturedByKey, maxTextCount: 8)
        } else {
          for alert in alerts {
            capture(alert, into: &capturedByKey, maxTextCount: 20)
          }
        }
      }
    }

    let currentKeys = Set(capturedByKey.keys)
    let newKeys =
      configuration.includeExisting && !isPrimed
      ? currentKeys
      : currentKeys.subtracting(activeNotificationKeys)

    if isPrimed || configuration.includeExisting {
      for key in newKeys.sorted() {
        guard let notification = capturedByKey[key],
          shouldDeliver(notification)
        else {
          continue
        }
        writer.append(notification)
        log(
          "捕获飞书通知：title=\(notification.title.debugDescription) "
            + "body=\(notification.body.debugDescription)"
        )
      }
    }

    activeNotificationKeys = currentKeys
    isPrimed = true
    pruneDeliveredFingerprints()
  }

  private func capture(
    _ element: AXUIElement,
    into result: inout [String: ParsedNotification],
    maxTextCount: Int
  ) {
    let texts = extractTexts(from: element)
    guard !texts.isEmpty, texts.count <= maxTextCount else {
      return
    }

    let description: String? = attribute(element, kAXDescriptionAttribute)
    guard
      let parsed = NotificationParser.parse(
        description: description,
        texts: texts,
        allowedApps: configuration.allowedApps
      )
    else {
      return
    }

    let key = "\(UInt(CFHash(element))):\(parsed.fingerprint)"
    result[key] = parsed
  }

  private func findAlertElements(
    in element: AXUIElement,
    depth: Int,
    visited: inout Set<UInt>
  ) -> [AXUIElement] {
    guard depth <= 9 else {
      return []
    }

    let id = UInt(CFHash(element))
    guard visited.insert(id).inserted else {
      return []
    }

    let subrole: String? = attribute(element, kAXSubroleAttribute)
    if subrole == "AXNotificationCenterAlert" {
      return [element]
    }

    var result: [AXUIElement] = []
    let children: [AXUIElement] =
      attribute(
        element,
        kAXChildrenAttribute
      ) ?? []
    for child in children {
      result.append(
        contentsOf: findAlertElements(
          in: child,
          depth: depth + 1,
          visited: &visited
        ))
    }
    return result
  }

  private func extractTexts(
    from element: AXUIElement,
    depth: Int = 0,
    visited: inout Set<UInt>,
    seenTexts: inout Set<String>
  ) -> [String] {
    guard depth <= 7 else {
      return []
    }

    let id = UInt(CFHash(element))
    guard visited.insert(id).inserted else {
      return []
    }

    var values: [String] = []
    let attributes = [
      kAXValueAttribute,
      kAXTitleAttribute,
      kAXDescriptionAttribute,
    ]

    for name in attributes {
      let value: String? = attribute(element, name)
      if let value {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty,
          trimmed.count <= 500,
          seenTexts.insert(trimmed).inserted
        {
          values.append(trimmed)
        }
      }
    }

    let children: [AXUIElement] =
      attribute(
        element,
        kAXChildrenAttribute
      ) ?? []
    for child in children {
      values.append(
        contentsOf: extractTexts(
          from: child,
          depth: depth + 1,
          visited: &visited,
          seenTexts: &seenTexts
        ))
    }
    return values
  }

  private func extractTexts(from element: AXUIElement) -> [String] {
    var visited = Set<UInt>()
    var seenTexts = Set<String>()
    return extractTexts(
      from: element,
      visited: &visited,
      seenTexts: &seenTexts
    )
  }

  private func attribute<T>(_ element: AXUIElement, _ name: String) -> T? {
    var value: AnyObject?
    let result = AXUIElementCopyAttributeValue(element, name as CFString, &value)
    guard result == .success, let value else {
      return nil
    }
    return value as? T
  }

  private func shouldDeliver(_ notification: ParsedNotification) -> Bool {
    let now = Date()
    if let lastDelivered = lastDeliveredAt[notification.fingerprint],
      now.timeIntervalSince(lastDelivered) < 3
    {
      return false
    }
    lastDeliveredAt[notification.fingerprint] = now
    return true
  }

  private func pruneDeliveredFingerprints() {
    let now = Date()
    lastDeliveredAt = lastDeliveredAt.filter {
      now.timeIntervalSince($0.value) < 30
    }
  }

  private func log(_ message: String, always: Bool = false) {
    guard configuration.debug || always else {
      return
    }
    fputs("[飞书通知验证器] \(message)\n", stderr)
    fflush(stderr)
  }
}

private func environmentStatus(prompt: Bool) -> [String: Any] {
  let accessibility = AXIsProcessTrustedWithOptions(
    [
      "AXTrustedCheckOptionPrompt": prompt
    ] as CFDictionary)
  let runningApps = NSWorkspace.shared.runningApplications
  let notificationProcesses = runningApps.compactMap { app -> String? in
    guard let identifier = app.bundleIdentifier,
      identifier == "com.apple.notificationcenterui"
        || identifier == "com.apple.UserNotificationCenter"
    else {
      return nil
    }
    return "\(identifier):\(app.processIdentifier)"
  }
  let larkRunning = !NSRunningApplication.runningApplications(
    withBundleIdentifier: "com.electron.lark"
  ).isEmpty

  return [
    "accessibility_trusted": accessibility,
    "lark_running": larkRunning,
    "notification_processes": notificationProcesses,
    "macos_version": ProcessInfo.processInfo.operatingSystemVersionString,
  ]
}

private func printJSON(_ payload: [String: Any], to handle: FileHandle) {
  guard
    let data = try? JSONSerialization.data(
      withJSONObject: payload,
      options: [.prettyPrinted, .sortedKeys]
    )
  else {
    return
  }
  handle.write(data)
  handle.write(Data([0x0A]))
}

do {
  let configuration = try Configuration.parse(
    arguments: Array(CommandLine.arguments.dropFirst())
  )

  if configuration.checkOnly {
    let status = environmentStatus(prompt: configuration.promptPermission)
    printJSON(status, to: .standardOutput)
    exit((status["accessibility_trusted"] as? Bool) == true ? 0 : 2)
  }

  let probe = try NotificationProbe(configuration: configuration)
  try probe.run()
} catch {
  printJSON(
    [
      "type": "error",
      "message": String(describing: error),
    ], to: .standardError)
  exit(1)
}
