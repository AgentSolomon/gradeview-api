import SwiftUI

// MARK: - Subscription Announcement Banner
// Shows on first open after update — "GradeView is going subscription"
// Dismissible, stores dismissed state in UserDefaults
struct SubscriptionAnnouncementBanner: View {
    @EnvironmentObject var subscriptionManager: SubscriptionManager
    @State private var isDismissed = false

    private let dismissedKey = "gradeview_subscription_banner_dismissed"

    var shouldShow: Bool {
        !isDismissed
        && !UserDefaults.standard.bool(forKey: dismissedKey)
        && !subscriptionManager.isSubscribed
    }

    var body: some View {
        if shouldShow {
            VStack(spacing: 0) {
                HStack(alignment: .top, spacing: 12) {
                    Image(systemName: "sparkles")
                        .foregroundColor(Color(hex: "#6c8fff"))
                        .font(.title3)
                        .padding(.top, 2)

                    VStack(alignment: .leading, spacing: 4) {
                        Text("GradeView is getting even better")
                            .font(.subheadline)
                            .fontWeight(.semibold)

                        if subscriptionManager.isGrandfathered {
                            Text("We're adding a subscription plan to support 500+ schools. As an early user, you get \(SubscriptionManager.grandfatherDays) days free — no credit card required. 🎉")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        } else {
                            Text("We're adding a subscription plan to support 500+ schools. You get \(SubscriptionManager.freeSearchLimit) free searches per month, or go Pro for less than a coffee.")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }

                    Button {
                        withAnimation {
                            isDismissed = true
                            UserDefaults.standard.set(true, forKey: dismissedKey)
                        }
                    } label: {
                        Image(systemName: "xmark")
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .padding(6)
                            .background(Color(.systemGray5))
                            .clipShape(Circle())
                    }
                }
                .padding()
                .background(Color(hex: "#6c8fff").opacity(0.08))
                .overlay(
                    Rectangle()
                        .frame(height: 1)
                        .foregroundColor(Color(hex: "#6c8fff").opacity(0.15)),
                    alignment: .bottom
                )
            }
            .transition(.move(edge: .top).combined(with: .opacity))
        }
    }
}

// MARK: - Search Limit Banner
// Shows inside SearchView when user has 1 free search left or has hit the limit
struct SearchLimitBanner: View {
    @EnvironmentObject var subscriptionManager: SubscriptionManager
    @State private var showPaywall = false

    var body: some View {
        if !subscriptionManager.isSubscribed && !subscriptionManager.isGrandfathered {
            let remaining = subscriptionManager.freeSearchesRemaining

            Group {
                if remaining == 0 {
                    HStack {
                        Image(systemName: "lock.fill")
                            .foregroundColor(.orange)
                        Text("You've used all \(SubscriptionManager.freeSearchLimit) free searches this month.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        Spacer()
                        Button("Go Pro") { showPaywall = true }
                            .font(.caption)
                            .fontWeight(.semibold)
                            .foregroundColor(Color(hex: "#6c8fff"))
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                    .background(Color.orange.opacity(0.06))
                } else if remaining == 1 {
                    HStack {
                        Image(systemName: "exclamationmark.circle")
                            .foregroundColor(Color(hex: "#6c8fff"))
                        Text("1 free search left this month.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        Spacer()
                        Button("Subscribe") { showPaywall = true }
                            .font(.caption)
                            .fontWeight(.semibold)
                            .foregroundColor(Color(hex: "#6c8fff"))
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                    .background(Color(hex: "#6c8fff").opacity(0.06))
                }
            }
            .sheet(isPresented: $showPaywall) {
                PaywallView()
                    .environmentObject(subscriptionManager)
            }
        }
    }
}
