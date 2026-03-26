import SwiftUI

struct SchoolPickerView: View {
    @State private var schools: [School] = []
    @State private var isLoading = true
    @State private var selectedSchool: School? = nil

    var body: some View {
        NavigationStack {
            Group {
                if isLoading {
                    VStack(spacing: 16) {
                        ProgressView()
                        Text("Loading schools...")
                            .foregroundColor(.secondary)
                    }
                } else if schools.isEmpty {
                    VStack(spacing: 12) {
                        Image(systemName: "building.columns")
                            .font(.system(size: 48))
                            .foregroundColor(.secondary)
                        Text("No schools available")
                            .font(.headline)
                        Text("Make sure the backend is running.")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                } else {
                    ScrollView {
                        VStack(spacing: 0) {
                            // Header
                            VStack(spacing: 8) {
                                Image(systemName: "graduationcap.fill")
                                    .font(.system(size: 44))
                                    .foregroundColor(.blue)
                                Text("GradeView")
                                    .font(.largeTitle)
                                    .fontWeight(.bold)
                                Text("Select a university to explore grade distributions")
                                    .font(.subheadline)
                                    .foregroundColor(.secondary)
                                    .multilineTextAlignment(.center)
                            }
                            .padding(.vertical, 32)
                            .padding(.horizontal)

                            // School list
                            VStack(spacing: 12) {
                                ForEach(schools) { school in
                                    NavigationLink(destination: SearchView(school: school)) {
                                        SchoolCard(school: school)
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                            .padding(.horizontal)
                        }
                    }
                }
            }
            .navigationBarHidden(true)
            .task { await loadSchools() }
        }
    }

    func loadSchools() async {
        isLoading = true
        do {
            let fetched = try await APIClient.shared.getSchools()
            await MainActor.run {
                schools = fetched
                isLoading = false
            }
        } catch {
            await MainActor.run {
                isLoading = false
            }
        }
    }
}

struct SchoolCard: View {
    let school: School

    var body: some View {
        HStack(spacing: 16) {
            // Color badge
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(hex: school.color))
                .frame(width: 52, height: 52)
                .overlay(
                    Text(school.short.prefix(2))
                        .font(.headline)
                        .fontWeight(.bold)
                        .foregroundColor(.white)
                )

            VStack(alignment: .leading, spacing: 4) {
                Text(school.name)
                    .font(.subheadline)
                    .fontWeight(.semibold)
                    .foregroundColor(.primary)
                    .multilineTextAlignment(.leading)
                Text(school.short)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Spacer()

            Image(systemName: "chevron.right")
                .font(.caption)
                .foregroundColor(.secondary)
        }
        .padding(16)
        .background(Color(.secondarySystemBackground))
        .cornerRadius(16)
    }
}
