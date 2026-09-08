import Foundation

public struct ParsedNotification: Equatable, Sendable {
  public let app: String
  public let title: String
  public let body: String
  public let subtitle: String
  public let rawTexts: [String]

  public init(
    app: String,
    title: String,
    body: String,
    subtitle: String,
    rawTexts: [String]
  ) {
    self.app = app
    self.title = title
    self.body = body
    self.subtitle = subtitle
    self.rawTexts = rawTexts
  }

  public var fingerprint: String {
    let source = ([app, title, body, subtitle] + rawTexts)
      .joined(separator: "\u{1F}")
    return StableFingerprint.fnv1a64(source)
  }
}

public enum NotificationParser {
  public static let defaultAllowedApps: Set<String> = [
    "lark",
    "feishu",
    "飞书",
  ]

  private static let ignoredLabels: Set<String> = [
    "close",
    "options",
    "reply",
    "view",
    "show more",
    "show less",
    "clear",
    "clear all",
    "alert",
    "notification center",
    "关闭",
    "选项",
    "回复",
    "查看",
    "显示更多",
    "显示更少",
    "清除",
    "全部清除",
    "提醒",
    "通知中心",
  ]

  public static func parse(
    description: String?,
    texts: [String],
    allowedApps: Set<String> = defaultAllowedApps
  ) -> ParsedNotification? {
    let normalizedAllowedApps = Set(allowedApps.map(normalizeAppName))
    func isAllowedApp(_ value: String) -> Bool {
      normalizedAllowedApps.contains(normalizeAppName(value))
    }
    func isAllowedDescription(_ parts: [String]) -> Bool {
      parts.count > 1 && parts.first.map(isAllowedApp) == true
    }

    let cleanTexts = sanitize(texts)
    let descriptionParts = splitDescription(description)
    let embeddedDescriptionParts: [String]? = cleanTexts.lazy.compactMap {
      value -> [String]? in
      let parts = splitDescription(value)
      guard isAllowedDescription(parts) else {
        return nil
      }
      return parts
    }.first

    let selectedDescriptionParts: [String]?
    if isAllowedDescription(descriptionParts) {
      selectedDescriptionParts = descriptionParts
    } else {
      selectedDescriptionParts = embeddedDescriptionParts
    }

    let describedApp = selectedDescriptionParts?.first
    let textApp = cleanTexts.first { candidate in
      isAllowedApp(candidate)
    }

    guard let app = describedApp ?? textApp else {
      return nil
    }

    let contentTexts = cleanTexts.filter { value in
      if isAllowedApp(value) {
        return false
      }

      let parts = splitDescription(value)
      return !isAllowedDescription(parts)
    }

    let descriptionContent = Array((selectedDescriptionParts ?? []).dropFirst())
      .filter { normalizeAppName($0) != "stacked" }
    let content = contentTexts.isEmpty ? descriptionContent : contentTexts

    guard !content.isEmpty else {
      return nil
    }

    return ParsedNotification(
      app: app,
      title: content[safe: 0] ?? "",
      body: content[safe: 1] ?? "",
      subtitle: content[safe: 2] ?? "",
      rawTexts: cleanTexts
    )
  }

  private static func sanitize(_ values: [String]) -> [String] {
    var seen = Set<String>()
    var result: [String] = []

    for value in values {
      let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
      guard !trimmed.isEmpty, trimmed.count <= 500 else {
        continue
      }

      let normalized = trimmed.lowercased()
      guard !ignoredLabels.contains(normalized), !seen.contains(trimmed) else {
        continue
      }

      seen.insert(trimmed)
      result.append(trimmed)
    }

    return result
  }

  private static func splitDescription(_ value: String?) -> [String] {
    sanitize(value?.split(separator: ",").map(String.init) ?? [])
  }

  private static func normalizeAppName(_ value: String) -> String {
    value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
  }
}

public enum StableFingerprint {
  public static func fnv1a64(_ value: String) -> String {
    var hash: UInt64 = 14_695_981_039_346_656_037
    let prime: UInt64 = 1_099_511_628_211

    for byte in value.utf8 {
      hash ^= UInt64(byte)
      hash &*= prime
    }

    return String(format: "%016llx", hash)
  }
}

public struct QQNotificationEvent: Codable, Equatable, Sendable {
  public let schema_version: Int
  public let type: String
  public let event_id: String
  public let bundle_id: String
  public let observed_at: String
  public let title: String
  public let body: String
  public let subtitle: String

  public init(notification: ParsedNotification) {
    schema_version = 1
    type = "qq_notification"
    event_id = UUID().uuidString
    bundle_id = "com.tencent.qq"
    observed_at = ISO8601DateFormatter().string(from: Date())
    title = notification.title
    body = notification.body
    subtitle = notification.subtitle
  }
}

public struct QQNotificationTracker {
  private var visibleKeys = Set<String>()

  public init() {}

  public mutating func observe(_ notifications: [String: ParsedNotification], emit: Bool) -> [QQNotificationEvent] {
    let qq = notifications.filter { $0.value.app.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "qq" }
    let currentKeys = Set(qq.keys)
    defer { visibleKeys = currentKeys }
    guard emit else { return [] }
    // 仅对仍可见的同一通知节点去重；不同节点的同文消息必须保留。
    return currentKeys.subtracting(visibleKeys).sorted().compactMap { key in
      qq[key].map { QQNotificationEvent(notification: $0) }
    }
  }
}

extension Array {
  fileprivate subscript(safe index: Index) -> Element? {
    indices.contains(index) ? self[index] : nil
  }
}
