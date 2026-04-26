import SwiftUI
import StoreKit

// MARK: - Paywall View
// Shown when a free user hits their search limit
struct PaywallView: View {
    @EnvironmentObject var subscriptionManager: SubscriptionManager
    @Environment(\.dismiss) var dismiss

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 28) {

                    // Header
                    VStack(spacing: 12) {
                        HStack(spacing: 0) {
                            Text("Grade")
                                .font(.system(size: 32, weight: .heavy))
                                .foregroundColor(.primary)
                            Text("View")
                                .font(.system(size: 32, weight: .heavy))
                                .foregroundColor(Color(hex: "#6c8fff"))
                            Text(" Pro")
                                .font(.system(size: 32, weight: .heavy))
                                .foregroundColor(Color(hex: "#6c8fff"))
                        }
                        Text("See every grade, at every school.")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    .padding(.top, 12)

                    // Grandfather banner (if eligible)
                    if subscriptionManager.isGrandfathered {
                        GrandfatherBanner(daysRemaining: subscriptionManager.grandfatherDaysRemaining)
                    }

                    // Free tier used up message (if not grandfathered)
                    if !subscriptionManager.isGrandfathered {
                        VStack(spacing: 8) {
                            Image(systemName: "chart.bar.fill")
                                .font(.system(size: 40))
                                .foregroundStyle(Color(hex: "#6c8fff").gradient)
                            Text("You've used your \(SubscriptionManager.freeSearchLimit) free searches this month")
                                .font(.headline)
                                .multilineTextAlignment(.center)
                            Text("Subscribe to keep searching — less than a coffee per month.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                        }
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(16)
                        .padding(.horizontal)
                    }

                    // Feature list
                    VStack(spacing: 14) {
                        FeatureRow(icon: "building.columns.fill",
                                   color: .indigo,
                                   title: "500+ universities",
                                   subtitle: "The largest grade database of any app")
                        FeatureRow(icon: "person.fill",
                                   color: .blue,
                                   title: "Professor-level data",
                                   subtitle: "See grade breakdowns by instructor")
                        FeatureRow(icon: "chart.bar.xaxis",
                                   color: .purple,
                                   title: "Full grade distributions",
                                   subtitle: "A through F, by semester")
                        FeatureRow(icon: "arrow.clockwise",
                                   color: .green,
                                   title: "Updated every semester",
                                   subtitle: "Always current data")
                    }
                    .padding(.horizontal)

                    // Price + CTA
                    VStack(spacing: 12) {
                        if let product = subscriptionManager.products.first {
                            Text(product.displayPrice + " / month")
                                .font(.title2)
                                .fontWeight(.bold)
                        } else {
                            Text("$1.99 / month")
                                .font(.title2)
                                .fontWeight(.bold)
                        }

                        Button {
                            Task { await subscriptionManager.purchase() }
                        } label: {
                            HStack {
                                if subscriptionManager.isPurchasing {
                                    ProgressView()
                                        .tint(.white)
                                } else {
                                    Text("Subscribe — Less than a coffee ☕")
                                        .fontWeight(.semibold)
                                }
                            }
                            .frame(maxWidth: .infinity)
                            .padding()
                            .background(Color(hex: "#6c8fff"))
                            .foregroundColor(.white)
                            .cornerRadius(14)
                        }
                        .disabled(subscriptionManager.isPurchasing)

                        Button("Restore Purchases") {
                            Task { await subscriptionManager.restorePurchases() }
                        }
                        .font(.footnote)
                        .foregroundColor(.secondary)

                        if let error = subscriptionManager.errorMessage {
                            Text(error)
                                .font(.caption)
                                .foregroundColor(.red)
                                .multilineTextAlignment(.center)
                        }

                        // Legal
                        Text("$1.99/month, billed monthly. Cancel anytime in Settings → Apple ID → Subscriptions. Payment charged to your Apple ID account at confirmation of purchase.")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal)
                    }
                    .padding(.horizontal)
                    .padding(.bottom, 24)
                }
            }
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Close") { dismiss() }
                }
            }
        }
    }
}

// MARK: - Grandfather Banner
struct GrandfatherBanner: View {
    let daysRemaining: Int

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: "gift.fill")
                .foregroundColor(.white)
                .font(.title3)
            VStack(alignment: .leading, spacing: 2) {
                Text("Early Adopter — \(daysRemaining) days free")
                    .font(.subheadline)
                    .fontWeight(.semibold)
                    .foregroundColor(.white)
                Text("You downloaded GradeView early. Enjoy free access — no card needed.")
                    .font(.caption)
                    .foregroundColor(.white.opacity(0.85))
            }
        }
        .padding()
        .background(
            LinearGradient(colors: [Color(hex: "#6c8fff"), Color(hex: "#8b5cf6")],
                           startPoint: .leading, endPoint: .trailing)
        )
        .cornerRadius(14)
        .padding(.horizontal)
    }
}

// MARK: - Feature Row
struct FeatureRow: View {
    let icon: String
    let color: Color
    let title: String
    let subtitle: String

    var body: some View {
        HStack(spacing: 14) {
            Image(systemName: icon)
                .font(.title3)
                .foregroundColor(color)
                .frame(width: 32)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.subheadline)
                    .fontWeight(.semibold)
                Text(subtitle)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            Spacer()
        }
    }
}
