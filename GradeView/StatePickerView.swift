import SwiftUI

struct StateGroup: Codable, Identifiable {
    var id: String { state }
    let state: String
    let schools: [School]
}

struct StatesResponse: Codable {
    let states: [StateGroup]
}

struct StatePickerView: View {
    @EnvironmentObject var subscriptionManager: SubscriptionManager
    @State private var stateGroups: [StateGroup] = []
    @State private var isLoading = true
    @State private var errorMessage: String?
    @State private var searchText = ""

    var allSchools: [School] {
        stateGroups.flatMap { $0.schools }
    }

    var filteredSchools: [School] {
        guard !searchText.isEmpty else { return [] }
        let q = searchText.lowercased()
        return allSchools.filter {
            $0.name.lowercased().contains(q) || $0.short.lowercased().contains(q) || ($0.state ?? "").lowercased().contains(q)
        }
    }

    var isSearching: Bool { !searchText.isEmpty }

    var body: some View {
        NavigationStack {
            Group {
                if isLoading {
                    ProgressView("Loading schools...")
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else if let error = errorMessage {
                    VStack(spacing: 12) {
                        Image(systemName: "exclamationmark.triangle")
                            .font(.largeTitle)
                            .foregroundColor(.orange)
                        Text(error)
                            .foregroundColor(.secondary)
                            .multilineTextAlignment(.center)
                        Button("Retry") { Task { await loadStates() } }
                            .buttonStyle(.borderedProminent)
                    }
                    .padding()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    List {
                        if !isSearching {
                            Section {
                                BrandHero()
                                    .listRowInsets(EdgeInsets())
                                    .listRowBackground(Color.clear)
                            }
                            Section {
                                SubscriptionAnnouncementBanner()
                                    .environmentObject(subscriptionManager)
                                    .listRowInsets(EdgeInsets())
                                    .listRowBackground(Color.clear)
                            }
                        }
                        if isSearching {
                            if filteredSchools.isEmpty {
                                Text("No schools found")
                                    .foregroundColor(.secondary)
                            } else {
                                ForEach(filteredSchools) { school in
                                    NavigationLink(destination: SearchView(school: school)
                                        .environmentObject(subscriptionManager)) {
                                        SchoolRow(school: school)
                                    }
                                }
                            }
                        } else {
                            ForEach(stateGroups) { group in
                                Section {
                                    ForEach(group.schools) { school in
                                        NavigationLink(destination: SearchView(school: school)
                                            .environmentObject(subscriptionManager)) {
                                            SchoolRow(school: school)
                                        }
                                    }
                                } header: {
                                    Text(stateName(group.state))
                                        .font(.subheadline)
                                        .fontWeight(.semibold)
                                        .textCase(nil)
                                }
                            }
                        }
                    }
                    .listStyle(.insetGrouped)
                }
            }
            .navigationTitle("")
            .navigationBarTitleDisplayMode(.inline)
            .searchable(text: $searchText, prompt: "Search schools...")
        }
        .task { await loadStates() }
    }

    func loadStates() async {
        isLoading = true
        errorMessage = nil
        do {
            let response: StatesResponse = try await APIClient.shared.fetch("/states")
            await MainActor.run {
                stateGroups = response.states
                isLoading = false
            }
        } catch {
            await MainActor.run {
                errorMessage = "Could not load schools.\nCheck your connection and try again."
                isLoading = false
            }
        }
    }

    func stateName(_ code: String) -> String {
        let names: [String: String] = [
            "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
            "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
            "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
            "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
            "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri",
            "MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey",
            "NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio",
            "OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
            "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
            "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
        ]
        return names[code] ?? code
    }
}

struct BrandHeader: View {
    var body: some View {
        HStack(spacing: 0) {
            Text("Grade")
                .font(.title3)
                .fontWeight(.heavy)
                .foregroundColor(.primary)
            Text("View")
                .font(.title3)
                .fontWeight(.heavy)
                .foregroundColor(Color(hex: "#6c8fff"))
        }
    }
}

struct BrandHero: View {
    var body: some View {
        VStack(spacing: 4) {
            HStack(spacing: 0) {
                Text("Grade")
                    .font(.system(size: 36, weight: .heavy))
                    .foregroundColor(.primary)
                Text("View")
                    .font(.system(size: 36, weight: .heavy))
                    .foregroundColor(Color(hex: "#6c8fff"))
            }
            Text("See the grades before you pick the class.")
                .font(.subheadline)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 20)
    }
}

struct SchoolRow: View {
    let school: School

    var body: some View {
        HStack(spacing: 12) {
            RoundedRectangle(cornerRadius: 8)
                .fill(Color(hex: school.color))
                .frame(width: 36, height: 36)
                .overlay(
                    Text(school.short.prefix(2))
                        .font(.caption)
                        .fontWeight(.bold)
                        .foregroundColor(.white)
                )
            VStack(alignment: .leading, spacing: 2) {
                Text(school.name)
                    .font(.body)
                    .fontWeight(.medium)
                Text(school.short)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
        .padding(.vertical, 2)
    }
}
