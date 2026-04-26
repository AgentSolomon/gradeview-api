import StoreKit
import SwiftUI

// MARK: - Product IDs
// Must match exactly what you create in App Store Connect
enum GradeViewProduct {
    static let monthlySubscription = "app.gradeview.pro.monthly"
}

// MARK: - Subscription Manager
@MainActor
class SubscriptionManager: ObservableObject {

    // Published state the UI observes
    @Published var isSubscribed = false
    @Published var isEligibleForFreeTier = true   // 3 free searches/month
    @Published var freeSearchesUsed = 0
    @Published var isGrandfathered = false         // downloaded before May 15 2026
    @Published var grandfatherDaysRemaining = 0
    @Published var products: [Product] = []
    @Published var isPurchasing = false
    @Published var errorMessage: String?

    // Constants
    static let freeSearchLimit = 3
    static let grandfatherCutoff = "2026-05-15"   // users before this date get 30 days free
    static let grandfatherDays = 30

    // UserDefaults keys
    private enum Keys {
        static let firstInstallDate = "gradeview_first_install_date"
        static let freeSearchesUsed = "gradeview_free_searches_used"
        static let freeSearchesMonth = "gradeview_free_searches_month"  // "2026-04" format
    }

    private var transactionListener: Task<Void, Error>?

    init() {
        // Record first install date if not already set
        if UserDefaults.standard.string(forKey: Keys.firstInstallDate) == nil {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withFullDate]
            UserDefaults.standard.set(formatter.string(from: Date()), forKey: Keys.firstInstallDate)
        }

        // Start listening for transactions
        transactionListener = listenForTransactions()

        Task {
            await loadProducts()
            await refreshSubscriptionStatus()
            checkFreeSearchReset()
        }
    }

    deinit {
        transactionListener?.cancel()
    }

    // MARK: - Load Products
    func loadProducts() async {
        do {
            let fetched = try await Product.products(for: [GradeViewProduct.monthlySubscription])
            products = fetched
        } catch {
            errorMessage = "Could not load subscription options."
        }
    }

    // MARK: - Subscription Status
    func refreshSubscriptionStatus() async {
        // Check grandfather status first
        checkGrandfatherStatus()
        if isGrandfathered { isSubscribed = true; return }

        // Check active subscription via StoreKit 2
        for await result in Transaction.currentEntitlements {
            guard case .verified(let transaction) = result else { continue }
            if transaction.productID == GradeViewProduct.monthlySubscription
                && transaction.revocationDate == nil {
                isSubscribed = true
                return
            }
        }
        isSubscribed = false
    }

    // MARK: - Grandfather Check
    private func checkGrandfatherStatus() {
        guard let installDateStr = UserDefaults.standard.string(forKey: Keys.firstInstallDate) else {
            isGrandfathered = false; return
        }

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate]
        guard let installDate = formatter.date(from: installDateStr),
              let cutoff = formatter.date(from: SubscriptionManager.grandfatherCutoff) else {
            isGrandfathered = false; return
        }

        if installDate < cutoff {
            // Eligible — check how many days of grace period remain
            let graceEnd = Calendar.current.date(
                byAdding: .day,
                value: SubscriptionManager.grandfatherDays,
                to: installDate
            ) ?? installDate

            let now = Date()
            if now < graceEnd {
                isGrandfathered = true
                let days = Calendar.current.dateComponents([.day], from: now, to: graceEnd).day ?? 0
                grandfatherDaysRemaining = max(0, days)
            } else {
                isGrandfathered = false
                grandfatherDaysRemaining = 0
            }
        } else {
            isGrandfathered = false
        }
    }

    // MARK: - Free Tier Search Tracking
    func checkFreeSearchReset() {
        let currentMonth = currentMonthKey()
        let savedMonth = UserDefaults.standard.string(forKey: Keys.freeSearchesMonth) ?? ""
        if savedMonth != currentMonth {
            // New month — reset counter
            UserDefaults.standard.set(0, forKey: Keys.freeSearchesUsed)
            UserDefaults.standard.set(currentMonth, forKey: Keys.freeSearchesMonth)
        }
        freeSearchesUsed = UserDefaults.standard.integer(forKey: Keys.freeSearchesUsed)
        isEligibleForFreeTier = freeSearchesUsed < SubscriptionManager.freeSearchLimit
    }

    func recordSearch() {
        guard !isSubscribed else { return }
        freeSearchesUsed += 1
        UserDefaults.standard.set(freeSearchesUsed, forKey: Keys.freeSearchesUsed)
        isEligibleForFreeTier = freeSearchesUsed < SubscriptionManager.freeSearchLimit
    }

    var canSearch: Bool {
        isSubscribed || isEligibleForFreeTier
    }

    var freeSearchesRemaining: Int {
        max(0, SubscriptionManager.freeSearchLimit - freeSearchesUsed)
    }

    private func currentMonthKey() -> String {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM"
        return f.string(from: Date())
    }

    // MARK: - Purchase
    func purchase() async {
        guard let product = products.first else {
            errorMessage = "Subscription not available. Try again later."
            return
        }
        isPurchasing = true
        errorMessage = nil

        do {
            let result = try await product.purchase()
            switch result {
            case .success(let verification):
                guard case .verified(let transaction) = verification else {
                    errorMessage = "Purchase could not be verified."
                    isPurchasing = false
                    return
                }
                await transaction.finish()
                await refreshSubscriptionStatus()
            case .userCancelled:
                break
            case .pending:
                errorMessage = "Purchase is pending approval."
            @unknown default:
                break
            }
        } catch {
            errorMessage = "Purchase failed: \(error.localizedDescription)"
        }
        isPurchasing = false
    }

    // MARK: - Restore Purchases (Apple requires this)
    func restorePurchases() async {
        isPurchasing = true
        do {
            try await AppStore.sync()
            await refreshSubscriptionStatus()
        } catch {
            errorMessage = "Restore failed: \(error.localizedDescription)"
        }
        isPurchasing = false
    }

    // MARK: - Transaction Listener
    private func listenForTransactions() -> Task<Void, Error> {
        Task.detached {
            for await result in Transaction.updates {
                guard case .verified(let transaction) = result else { continue }
                await MainActor.run {
                    Task { await self.refreshSubscriptionStatus() }
                }
                await transaction.finish()
            }
        }
    }
}
