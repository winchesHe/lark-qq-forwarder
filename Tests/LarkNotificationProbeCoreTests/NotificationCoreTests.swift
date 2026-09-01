import XCTest

@testable import LarkNotificationProbeCore

final class NotificationCoreTests: XCTestCase {
  func testParsesLarkStaticTexts() {
    let result = NotificationParser.parse(
      description: nil,
      texts: ["Lark", "研发群", "张三：构建完成"]
    )

    XCTAssertEqual(
      result,
      ParsedNotification(
        app: "Lark",
        title: "研发群",
        body: "张三：构建完成",
        subtitle: "",
        rawTexts: ["Lark", "研发群", "张三：构建完成"]
      )
    )
  }

  func testParsesDescriptionFallback() {
    let result = NotificationParser.parse(
      description: "飞书, 王小明, 收到请回复, stacked",
      texts: []
    )

    XCTAssertEqual(result?.app, "飞书")
    XCTAssertEqual(result?.title, "王小明")
    XCTAssertEqual(result?.body, "收到请回复")
    XCTAssertEqual(result?.subtitle, "")
  }

  func testFindsLarkDescriptionInChildNodeTexts() {
    let result = NotificationParser.parse(
      description: nil,
      texts: [
        "Notification Center",
        "Lark, 测试群, 王小明：第二条消息, stacked",
        "测试群",
        "王小明：第二条消息",
      ]
    )

    XCTAssertEqual(result?.app, "Lark")
    XCTAssertEqual(result?.title, "测试群")
    XCTAssertEqual(result?.body, "王小明：第二条消息")
  }

  func testRejectsOtherApplications() {
    XCTAssertNil(
      NotificationParser.parse(
        description: "Mail, Weekly report",
        texts: ["Mail", "Weekly report"]
      ))
  }

  func testIgnoresNotificationButtons() {
    let result = NotificationParser.parse(
      description: nil,
      texts: ["Feishu", "产品群", "新消息", "Reply", "关闭"]
    )

    XCTAssertEqual(result?.rawTexts, ["Feishu", "产品群", "新消息"])
  }

  func testFingerprintIsStableAndContentSensitive() {
    let first = ParsedNotification(
      app: "Lark",
      title: "群聊",
      body: "消息 A",
      subtitle: "",
      rawTexts: ["Lark", "群聊", "消息 A"]
    )
    let same = first
    let different = ParsedNotification(
      app: "Lark",
      title: "群聊",
      body: "消息 B",
      subtitle: "",
      rawTexts: ["Lark", "群聊", "消息 B"]
    )

    XCTAssertEqual(first.fingerprint, same.fingerprint)
    XCTAssertNotEqual(first.fingerprint, different.fingerprint)
    XCTAssertEqual(first.fingerprint.count, 16)
  }
}
