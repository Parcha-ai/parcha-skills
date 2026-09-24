import AppKit
import AuthenticationServices
import CryptoKit
import Foundation
import Security
import SwiftUI

struct Brain: Codable, Identifiable {
    let tenant_id: String
    let slug: String
    let brain_kind: String
    let display_name: String
    let permission: String
    var id: String { tenant_id }
}

struct Installation: Codable, Identifiable {
    let id: String
    let tenant_id: String
    let connector_id: String
    let source_id: String
    let device_id: String?
    let state: String
}

struct ProviderState: Codable, Identifiable {
    let id: String
    let status: String
}

struct Principal: Codable {
    let id: String
}

struct ControlState: Codable {
    let principal: Principal
    let brains: [Brain]
    let providers: [ProviderState]
    let installations: [Installation]
}

struct LocalSource: Codable {
    let enabled: Bool
    let health: String
    let lag_seconds: Int?
    let state_present: Bool
    let privacy_mode: String?
    let surface: String
    let connector_id: String
}

struct LocalStatus: Codable {
    let sources: [String: LocalSource]
}

struct RouteInfo: Codable {
    let connector_id: String
    let source_id: String
    let keychain_service: String
    let keychain_account: String
    let privacy_mode: String
}

struct DeviceRoute: Codable {
    let installation_id: String
    let token: String
}

struct TransitionResponse: Codable {
    let installation_id: String
    let state: String
}

struct LifecycleResponse: Codable {
    let enabled: Bool
}

struct RouteApplyResponse: Codable {
    let canonical_v2: Bool
}

enum AdminFailure: LocalizedError {
    case closed(String)

    var errorDescription: String? {
        switch self {
        case .closed(let code): return code.replacingOccurrences(of: "_", with: " ")
        }
    }
}

final class NoRedirectSessionDelegate: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

@MainActor
final class RecallAdminModel: NSObject, ObservableObject, ASWebAuthenticationPresentationContextProviding {
    @Published var endpoint =
        UserDefaults.standard.string(forKey: "recall.endpoint")
        ?? "https://recall-mcp-parcha.onrender.com"
    @Published var adminKey = ""
    @Published var control: ControlState?
    @Published var local: LocalStatus?
    @Published var destinations: [String: String] = [:]
    @Published var busy: Set<String> = []
    @Published var error = ""
    @Published var connected = false
    @Published var signingIn = false
    private var authentication: ASWebAuthenticationSession?

    let deviceID: String
    private let cookies = HTTPCookieStorage()
    private lazy var session: URLSession = {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpCookieStorage = cookies
        configuration.httpShouldSetCookies = true
        configuration.timeoutIntervalForRequest = 20
        return URLSession(configuration: configuration,
                          delegate: NoRedirectSessionDelegate(), delegateQueue: nil)
    }()

    override init() {
        if let existing = UserDefaults.standard.string(forKey: "recall.device-id") {
            deviceID = existing
        } else {
            let suffix = UUID().uuidString.lowercased().replacingOccurrences(
                of: "-", with: ""
            )
            deviceID = "mac-" + String(suffix.prefix(16))
            UserDefaults.standard.set(deviceID, forKey: "recall.device-id")
        }
        super.init()
    }

    nonisolated func presentationAnchor(for session: ASWebAuthenticationSession) -> ASPresentationAnchor {
        if Thread.isMainThread {
            return MainActor.assumeIsolated { NSApp.keyWindow ?? NSApp.windows.first ?? NSWindow() }
        }
        return DispatchQueue.main.sync {
            MainActor.assumeIsolated { NSApp.keyWindow ?? NSApp.windows.first ?? NSWindow() }
        }
    }

    private static func randomToken() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else {
            throw AdminFailure.closed("secure_random_unavailable")
        }
        return Data(bytes).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    nonisolated static func callbackCode(_ url: URL, state: String) throws -> String {
        guard let parts = URLComponents(url: url, resolvingAgainstBaseURL: false),
              parts.scheme == "ai.parcha.recall", parts.host == "oauth",
              parts.path.isEmpty, parts.user == nil, parts.password == nil,
              parts.port == nil, parts.fragment == nil,
              let items = parts.queryItems, items.count == 2,
              items.filter({ $0.name == "state" && $0.value == state }).count == 1,
              let code = items.first(where: { $0.name == "code" })?.value,
              code.range(of: "^[A-Za-z0-9_-]{32,256}$", options: .regularExpression) != nil
        else { throw AdminFailure.closed("sign_in_callback_invalid") }
        return code
    }

    func signIn() async {
        guard !signingIn else { return }
        signingIn = true
        error = ""
        defer { signingIn = false; authentication = nil }
        do {
            guard let base = validatedEndpoint() else {
                throw AdminFailure.closed("endpoint_invalid")
            }
            let verifier = try Self.randomToken()
            let state = try Self.randomToken()
            let challenge = Data(SHA256.hash(data: Data(verifier.utf8))).base64EncodedString()
                .replacingOccurrences(of: "+", with: "-")
                .replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")
            var login = URLComponents(url: base.appending(path: "/admin/native/login"),
                                      resolvingAgainstBaseURL: false)!
            login.queryItems = [URLQueryItem(name: "state", value: state),
                               URLQueryItem(name: "code_challenge", value: challenge)]
            let callback: URL = try await withCheckedThrowingContinuation { continuation in
                let auth = ASWebAuthenticationSession(url: login.url!,
                                                     callbackURLScheme: "ai.parcha.recall") { url, error in
                    if let url { continuation.resume(returning: url) }
                    else { continuation.resume(throwing: error ?? AdminFailure.closed("sign_in_cancelled")) }
                }
                auth.presentationContextProvider = self
                authentication = auth
                if !auth.start() {
                    continuation.resume(throwing: AdminFailure.closed("sign_in_unavailable"))
                }
            }
            let code = try Self.callbackCode(callback, state: state)
            // The verifier stays in this process. The callback carries no session or collector token.
            let body = try JSONSerialization.data(withJSONObject: ["code": code, "code_verifier": verifier])
            _ = try await request(base.appending(path: "/admin/api/v1/native/session"),
                                  method: "POST", body: body, csrf: false) as [String: String]
            UserDefaults.standard.set(endpoint, forKey: "recall.endpoint")
            try await refresh()
            connected = true
        } catch {
            connected = false
            self.error = error.localizedDescription
        }
    }

    func connect() async {
        error = ""
        do {
            guard let base = validatedEndpoint() else {
                throw AdminFailure.closed("endpoint_invalid")
            }
            let key = adminKey.isEmpty
                ? try Keychain.read(service: "ai.parcha.recall.admin", account: "owner")
                : adminKey
            let body = try JSONSerialization.data(withJSONObject: ["token": key])
            _ = try await request(
                base.appending(path: "/admin/api/v1/session"),
                method: "POST",
                body: body,
                csrf: false
            ) as [String: String]
            if !adminKey.isEmpty {
                try Keychain.write(
                    adminKey, service: "ai.parcha.recall.admin", account: "owner"
                )
                adminKey = ""
            }
            UserDefaults.standard.set(endpoint, forKey: "recall.endpoint")
            connected = true
            try await refresh()
        } catch {
            connected = false
            self.error = error.localizedDescription
        }
    }

    func refresh() async throws {
        async let remote: ControlState = request(
            validatedEndpoint()!.appending(path: "/admin/api/v1/state"),
            method: "GET",
            body: nil,
            csrf: false
        )
        async let localValue: LocalStatus = runRecall(["mac-status"])
        let (newControl, newLocal) = try await (remote, localValue)
        control = newControl
        local = newLocal
        for (name, source) in newLocal.sources {
            if let installation = newControl.installations.first(where: {
                $0.device_id == deviceID && $0.connector_id == source.connector_id
                    && !["revoked", "uninstalled"].contains($0.state)
            }) {
                destinations[name] = installation.tenant_id
            } else if destinations[name] == nil {
                destinations[name] = newControl.brains.first?.tenant_id
            }
        }
    }

    func setEnabled(_ enabled: Bool, source name: String) async {
        guard let source = local?.sources[name], !busy.contains(name) else { return }
        busy.insert(name)
        defer { busy.remove(name) }
        error = ""
        do {
            if enabled {
                try await enable(name: name, source: source)
            } else {
                try await pause(name: name, source: source)
            }
            try await refresh()
        } catch {
            self.error = error.localizedDescription
            try? await refresh()
        }
    }

    func isEnabled(_ name: String, source: LocalSource) -> Bool {
        source.enabled && (control?.installations.contains {
            $0.device_id == deviceID && $0.connector_id == source.connector_id
                && $0.tenant_id == destinations[name] && $0.state == "enabled"
        } ?? false)
    }

    func reroute(source name: String) async {
        guard local?.sources[name]?.enabled == true,
              let source = local?.sources[name]
        else { return }
        await setEnabled(true, source: name)
        if !source.enabled { return }
    }

    func openWebAdmin() {
        guard let base = validatedEndpoint() else { return }
        NSWorkspace.shared.open(base.appending(path: "/admin"))
    }

    private func enable(name: String, source: LocalSource) async throws {
        guard let tenant = destinations[name] ?? control?.brains.first?.tenant_id else {
            throw AdminFailure.closed("brain_destination_missing")
        }
        let active = control?.installations.first(where: {
            $0.device_id == deviceID && $0.connector_id == source.connector_id
                && !["revoked", "uninstalled"].contains($0.state)
        })
        let route: RouteInfo = try await runRecall([
            "mac-route-info", "--source", name,
        ])
        if source.enabled {
            let _: LifecycleResponse = try await runRecall([
                "mac-pause", "--source", name,
            ])
        }
        if active?.tenant_id == tenant && active?.state == "paused" {
            try await transition(active!.id, action: "resume")
        } else if active?.tenant_id == tenant && active?.state == "enabled"
                    && source.enabled {
            // Restart below so retained LaunchAgent authority changes take effect.
        } else if active?.tenant_id != tenant || active == nil {
            let payload: [String: Any] = [
                "connector_id": route.connector_id,
                "tenant_id": tenant,
                "device_id": deviceID,
                "source_id": route.source_id,
                "privacy_mode": route.privacy_mode,
                "selectors": [:] as [String: String],
            ]
            let data = try JSONSerialization.data(withJSONObject: payload)
            let response: DeviceRoute = try await request(
                validatedEndpoint()!.appending(
                    path: "/admin/api/v1/device/installations"
                ),
                method: "POST",
                body: data,
                csrf: true
            )
            try Keychain.write(
                response.token,
                service: route.keychain_service,
                account: route.keychain_account
            )
        }
        guard let principal = control?.principal.id else {
            throw AdminFailure.closed("brain_principal_missing")
        }
        let applied: RouteApplyResponse = try await runRecall([
            "mac-route-apply", "--source", name,
            "--tenant-id", tenant, "--principal-id", principal,
        ])
        guard applied.canonical_v2 else {
            throw AdminFailure.closed("canonical_route_apply_failed")
        }
        let _: LifecycleResponse = try await runRecall([
            "mac-resume", "--source", name,
        ])
    }

    private func pause(name: String, source: LocalSource) async throws {
        let _: LifecycleResponse = try await runRecall([
            "mac-pause", "--source", name,
        ])
        if let route = control?.installations.first(where: {
            $0.device_id == deviceID && $0.connector_id == source.connector_id
                && $0.state == "enabled"
        }) {
            try await transition(route.id, action: "pause")
        }
    }

    private func transition(_ id: String, action: String) async throws {
        let body = try JSONSerialization.data(withJSONObject: ["action": action])
        let _: TransitionResponse = try await request(
            validatedEndpoint()!.appending(
                path: "/admin/api/v1/installations/\(id)/actions"
            ),
            method: "POST",
            body: body,
            csrf: true
        )
    }

    private func validatedEndpoint() -> URL? {
        guard let url = URL(string: endpoint),
              url.scheme == "https",
              url.host != nil,
              ["", "/"].contains(url.path),
              url.user == nil,
              url.password == nil,
              url.query == nil,
              url.fragment == nil
        else { return nil }
        return url
    }

    private func csrfValue() -> String? {
        cookies.cookies?.first(where: { $0.name == "recall_admin_csrf" })?.value
    }

    private func request<T: Decodable>(
        _ url: URL,
        method: String,
        body: Data?,
        csrf: Bool
    ) async throws -> T {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if csrf {
            guard let value = csrfValue() else {
                throw AdminFailure.closed("admin_csrf_missing")
            }
            request.setValue(value, forHTTPHeaderField: "X-Recall-CSRF")
        }
        let (data, response) = try await session.data(for: request)
        guard data.count <= 1_000_000,
              let http = response as? HTTPURLResponse
        else { throw AdminFailure.closed("admin_response_invalid") }
        if !(200..<300).contains(http.statusCode) {
            let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            throw AdminFailure.closed(value?["error"] as? String ?? "admin_request_failed")
        }
        return try JSONDecoder().decode(T.self, from: data)
    }

    private func runRecall<T: Decodable>(_ arguments: [String]) async throws -> T {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let executable = home
            .appending(path: "Library/Application Support/RecallBrain/bin/recall-brain")
        guard FileManager.default.isExecutableFile(atPath: executable.path) else {
            throw AdminFailure.closed("recall_utility_not_installed")
        }
        let process = Process()
        let output = Pipe()
        process.executableURL = executable
        process.arguments = arguments
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0, data.count <= 1_000_000 else {
            throw AdminFailure.closed("local_utility_failed")
        }
        return try JSONDecoder().decode(T.self, from: data)
    }
}

enum Keychain {
    static func write(_ value: String, service: String, account: String) throws {
        let identity: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]
        let attributes: [String: Any] = [
            kSecValueData as String: Data(value.utf8),
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlock,
        ]
        let status = SecItemUpdate(identity as CFDictionary, attributes as CFDictionary)
        if status == errSecItemNotFound {
            var create = identity
            attributes.forEach { create[$0.key] = $0.value }
            guard SecItemAdd(create as CFDictionary, nil) == errSecSuccess else {
                throw AdminFailure.closed("keychain_write_failed")
            }
        } else if status != errSecSuccess {
            throw AdminFailure.closed("keychain_write_failed")
        }
    }

    static func read(service: String, account: String) throws -> String {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data,
              let value = String(data: data, encoding: .utf8)
        else { throw AdminFailure.closed("admin_key_missing") }
        return value
    }
}

struct ContentView: View {
    @StateObject private var model = RecallAdminModel()

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            if model.connected {
                switchboard
            } else {
                connection
            }
        }
        .frame(minWidth: 860, minHeight: 620)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("RECALL / SWITCHBOARD").font(.caption.monospaced()).foregroundStyle(.secondary)
                Text("Personal memory. Company context. Explicit routes.")
                    .font(.title2.weight(.semibold))
            }
            Spacer()
            Circle().fill(model.connected ? .green : .orange).frame(width: 9, height: 9)
            Text(model.connected ? "CONNECTED" : "SIGN IN")
                .font(.caption.monospaced())
        }
        .padding(22)
    }

    private var connection: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Connect this Mac").font(.largeTitle.bold())
            Text("Sign in with the account already connected to your company brain. Then choose which sessions this Mac shares.")
                .foregroundStyle(.secondary)
            TextField("https://recall.example", text: $model.endpoint)
                .textFieldStyle(.roundedBorder)
                .disabled(model.signingIn)
            Button(model.signingIn ? "Waiting for sign-in…" : "Sign in with browser") {
                Task { await model.signIn() }
            }
            .buttonStyle(.borderedProminent)
            .disabled(model.signingIn)
            DisclosureGroup("Administrator key (optional)") {
                SecureField("Admin access key", text: $model.adminKey)
                    .textFieldStyle(.roundedBorder)
                Button("Connect with key") { Task { await model.connect() } }
                    .disabled(model.signingIn)
            }
            if !model.error.isEmpty {
                Text(model.error).foregroundStyle(.red)
            }
        }
        .frame(maxWidth: 520, alignment: .leading)
        .padding(48)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var switchboard: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 26) {
                brains
                localSources
                HStack {
                    VStack(alignment: .leading) {
                        Text("Remote integrations").font(.title2.bold())
                        Text("Google Workspace and future OAuth services use the same destinations.")
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("Open web admin ↗") { model.openWebAdmin() }
                }
                .padding(20)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14))
                if !model.error.isEmpty {
                    Text(model.error).foregroundStyle(.red)
                }
            }
            .padding(24)
        }
    }

    private var brains: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("DESTINATIONS").font(.caption.monospaced()).foregroundStyle(.secondary)
            HStack {
                ForEach(model.control?.brains ?? []) { brain in
                    VStack(alignment: .leading, spacing: 5) {
                        Text(brain.brain_kind.uppercased()).font(.caption.monospaced())
                        Text(brain.display_name).font(.title3.bold())
                        Text(brain.permission).foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(18)
                    .background(
                        brain.brain_kind == "personal"
                            ? Color.blue.opacity(0.12) : Color.green.opacity(0.12),
                        in: RoundedRectangle(cornerRadius: 14)
                    )
                }
            }
        }
    }

    private var localSources: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("THIS MAC").font(.caption.monospaced()).foregroundStyle(.secondary)
            Text("Local collectors").font(.title.bold())
            ForEach((model.local?.sources.keys.sorted() ?? []), id: \.self) { name in
                if let source = model.local?.sources[name],
                   source.health != "disabled" || source.state_present {
                    sourceRow(name, source)
                }
            }
        }
    }

    private func sourceRow(_ name: String, _ source: LocalSource) -> some View {
        HStack(spacing: 16) {
            VStack(alignment: .leading, spacing: 3) {
                Text(name.replacingOccurrences(of: "-", with: " ").capitalized)
                    .font(.headline)
                Text("\(source.health) · \(source.surface)")
                    .font(.caption).foregroundStyle(.secondary)
            }
            Spacer()
            Picker(
                "Destination",
                selection: Binding(
                    get: { model.destinations[name] ?? "" },
                    set: { value in
                        model.destinations[name] = value
                        Task { await model.reroute(source: name) }
                    }
                )
            ) {
                ForEach(model.control?.brains ?? []) { brain in
                    Text(brain.display_name).tag(brain.tenant_id)
                }
            }
            .frame(width: 190)
            Toggle(
                "",
                isOn: Binding(
                    get: { model.isEnabled(name, source: source) },
                    set: { value in Task { await model.setEnabled(value, source: name) } }
                )
            )
            .labelsHidden()
            .disabled(model.busy.contains(name))
        }
        .padding(15)
        .background(Color(nsColor: .controlBackgroundColor), in: RoundedRectangle(cornerRadius: 12))
    }
}

@main
enum RecallBrainAdminMain {
    static func main() {
        if CommandLine.arguments == [CommandLine.arguments[0], "--self-test"] {
            let controlJSON = """
            {"principal":{"id":"principal:synthetic"},"brains":[
              {"tenant_id":"tenant:personal:synthetic","slug":"personal","brain_kind":"personal","display_name":"Personal","permission":"owner"},
              {"tenant_id":"tenant:company:synthetic","slug":"company","brain_kind":"company","display_name":"Company","permission":"owner"}
            ],"providers":[],"installations":[]}
            """.data(using: .utf8)!
            let localJSON = """
            {"sources":{"codex":{"enabled":true,"health":"ready","lag_seconds":0,"state_present":true,"privacy_mode":"scrub","surface":"synthetic","connector_id":"local.codex"}}}
            """.data(using: .utf8)!
            do {
                let control = try JSONDecoder().decode(
                    ControlState.self, from: controlJSON
                )
                let local = try JSONDecoder().decode(
                    LocalStatus.self, from: localJSON
                )
                guard control.brains.count == 2,
                      local.sources["codex"]?.connector_id == "local.codex"
                else { throw AdminFailure.closed("self_test_contract_failed") }
                let state = String(repeating: "s", count: 43)
                let code = String(repeating: "c", count: 43)
                let callback = "ai.parcha.recall://oauth?state=\(state)&code=\(code)"
                guard try RecallAdminModel.callbackCode(URL(string: callback)!, state: state) == code
                else { throw AdminFailure.closed("self_test_callback_failed") }
                for invalid in [
                    callback.replacingOccurrences(of: "ai.parcha.recall:", with: "https:"),
                    callback.replacingOccurrences(of: "//oauth?", with: "//other?"),
                    callback + "&code=duplicate", callback + "#fragment",
                    callback.replacingOccurrences(of: state, with: "wrong-state"),
                ] {
                    if (try? RecallAdminModel.callbackCode(URL(string: invalid)!, state: state)) != nil {
                        throw AdminFailure.closed("self_test_callback_failed")
                    }
                }
                print(
                    #"{"architecture":"arm64","brains":2,"local_sources":1,"status":"pass"}"#
                )
                return
            } catch {
                fputs("{\"status\":\"fail\"}\n", stderr)
                exit(1)
            }
        }
        let application = NSApplication.shared
        let delegate = RecallApplicationDelegate()
        application.delegate = delegate
        application.setActivationPolicy(.regular)
        application.finishLaunching()
        delegate.showWindow()
        application.activate(ignoringOtherApps: true)
        application.run()
    }
}

final class RecallApplicationDelegate: NSObject, NSApplicationDelegate {
    private var window: NSWindow?

    func applicationDidFinishLaunching(_ notification: Notification) {
        showWindow()
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
    }

    func showWindow() {
        if let window {
            window.makeKeyAndOrderFront(nil)
            return
        }
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 980, height: 720),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "Recall Brain"
        window.center()
        window.contentView = NSHostingView(rootView: ContentView())
        window.makeKeyAndOrderFront(nil)
        self.window = window
    }

    func applicationShouldTerminateAfterLastWindowClosed(
        _ sender: NSApplication
    ) -> Bool {
        true
    }
}
