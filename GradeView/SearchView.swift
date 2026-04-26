import SwiftUI

struct SearchView: View {
    let school: School

    @EnvironmentObject var subscriptionManager: SubscriptionManager
    @State private var query = ""
    @State private var results: [SearchResult] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var hasSearched = false
    @State private var showPaywall = false

    var body: some View {
        VStack(spacing: 0) {

            // Search limit banner (shows when 1 left or 0 left)
            SearchLimitBanner()
                .environmentObject(subscriptionManager)

            // School banner
            HStack(spacing: 10) {
                RoundedRectangle(cornerRadius: 6)
                    .fill(Color(hex: school.color))
                    .frame(width: 28, height: 28)
                    .overlay(
                        Text(school.short.prefix(2))
                            .font(.caption)
                            .fontWeight(.bold)
                            .foregroundColor(.white)
                    )
                Text(school.short)
                    .font(.subheadline)
                    .fontWeight(.semibold)
                Spacer()

                // Pro badge for subscribers / grandfathered
                if subscriptionManager.isSubscribed {
                    ProBadge()
                }
            }
            .padding(.horizontal)
            .padding(.top, 8)

            // Search bar
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Image(systemName: "magnifyingglass")
                        .foregroundColor(.secondary)
                    TextField("e.g. CSCE 121 or BIOL 111", text: $query)
                        .textInputAutocapitalization(.characters)
                        .autocorrectionDisabled()
                        .submitLabel(.search)
                        .onSubmit { trySearch() }
                    if !query.isEmpty {
                        Button {
                            query = ""; results = []; hasSearched = false
                        } label: {
                            Image(systemName: "xmark.circle.fill")
                                .foregroundColor(.secondary)
                        }
                    }
                }
                .padding(10)
                .background(Color(.systemGray6))
                .cornerRadius(10)
                .padding(.horizontal)
            }
            .padding(.vertical, 8)

            if isLoading {
                Spacer()
                ProgressView("Searching...")
                Spacer()
            } else if let error = errorMessage {
                Spacer()
                VStack(spacing: 8) {
                    Image(systemName: "magnifyingglass")
                        .font(.largeTitle)
                        .foregroundColor(.secondary)
                    Text(error)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding()
                Spacer()
            } else if !hasSearched {
                Spacer()
                VStack(spacing: 12) {
                    Image(systemName: "chart.bar.fill")
                        .font(.system(size: 48))
                        .foregroundStyle(Color(hex: school.color).gradient)
                    Text("Search \(school.short)")
                        .font(.title2)
                        .fontWeight(.bold)
                    Text("Enter a department code and course number\nto see grade distributions.")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)

                    // Free search counter for non-subscribers
                    if !subscriptionManager.isSubscribed && !subscriptionManager.isGrandfathered {
                        Text("\(subscriptionManager.freeSearchesRemaining) free search\(subscriptionManager.freeSearchesRemaining == 1 ? "" : "es") remaining this month")
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .padding(.top, 4)
                    }
                }
                .padding()
                Spacer()
            } else {
                List(results) { result in
                    NavigationLink(destination: CourseDetailView(
                        department: result.department,
                        courseNum: result.course,
                        school: school
                    )) {
                        SearchResultRow(result: result)
                    }
                }
                .listStyle(.plain)
            }
        }
        .navigationTitle("Search Courses")
        .navigationBarTitleDisplayMode(.inline)
        .sheet(isPresented: $showPaywall) {
            PaywallView()
                .environmentObject(subscriptionManager)
        }
    }

    // MARK: - Gate search on subscription status
    func trySearch() {
        if subscriptionManager.canSearch {
            subscriptionManager.recordSearch()
            performSearch()
        } else {
            // Hit the limit — show paywall
            showPaywall = true
        }
    }

    func performSearch() {
        guard !query.trimmingCharacters(in: .whitespaces).isEmpty else { return }
        isLoading = true
        errorMessage = nil
        hasSearched = true

        Task {
            do {
                let response = try await APIClient.shared.searchCourses(query: query, school: school.id)
                await MainActor.run {
                    results = response.results
                    isLoading = false
                    if results.isEmpty {
                        errorMessage = "No results for \"\(query)\""
                    }
                }
            } catch {
                await MainActor.run {
                    errorMessage = "No results for \"\(query)\"\nTry a department code like CSCE or MATH."
                    isLoading = false
                }
            }
        }
    }
}

// MARK: - Pro Badge
struct ProBadge: View {
    var body: some View {
        Text("PRO")
            .font(.caption2)
            .fontWeight(.heavy)
            .foregroundColor(.white)
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(Color(hex: "#6c8fff"))
            .cornerRadius(5)
    }
}

// MARK: - Search Result Row (unchanged)
struct SearchResultRow: View {
    let result: SearchResult

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text("\(result.department) \(result.course)")
                    .font(.headline)
                Spacer()
                GPABadge(gpa: result.avg_gpa)
            }
            HStack(spacing: 12) {
                GradeChip(letter: "A", pct: result.pct_A, color: .green)
                GradeChip(letter: "B", pct: result.pct_B, color: .yellow)
                GradeChip(letter: "C", pct: result.pct_C, color: .orange)
                GradeChip(letter: "F", pct: result.pct_F, color: .red)
                Spacer()
                Text("\(result.total) students")
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }
        }
        .padding(.vertical, 4)
    }
}

struct GradeChip: View {
    let letter: String
    let pct: Double
    let color: Color

    var body: some View {
        HStack(spacing: 2) {
            Text(letter).fontWeight(.bold)
            Text(String(format: "%.0f%%", pct))
        }
        .font(.caption2)
        .foregroundColor(color)
    }
}

struct GPABadge: View {
    let gpa: Double

    var color: Color {
        switch gpa {
        case 3.5...: return .green
        case 3.0..<3.5: return .blue
        case 2.5..<3.0: return .orange
        default: return .red
        }
    }

    var body: some View {
        Text(String(format: "%.2f", gpa))
            .font(.caption)
            .fontWeight(.semibold)
            .foregroundColor(color)
            .padding(.horizontal, 8)
            .padding(.vertical, 3)
            .background(color.opacity(0.15))
            .cornerRadius(6)
    }
}
