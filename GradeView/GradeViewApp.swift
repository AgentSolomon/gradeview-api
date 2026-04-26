import SwiftUI

@main
struct GradeViewApp: App {
    @StateObject private var subscriptionManager = SubscriptionManager()

    var body: some Scene {
        WindowGroup {
            StatePickerView()
                .environmentObject(subscriptionManager)
        }
    }
}
