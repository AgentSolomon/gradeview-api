import SwiftUI

struct ProfessorDetailView: View {
    let professorName: String
    let department: String?

    @State private var profData: ProfessorResponse?
    @State private var isLoading = true
    @State private var errorMessage: String?

    init(professorName: String, department: String? = nil) {
        self.professorName = professorName
        self.department = department
    }

    var body: some View {
        Group {
            if isLoading {
                ProgressView("Loading...")
            } else if let error = errorMessage {
                VStack {
                    Image(systemName: "exclamationmark.triangle")
                        .font(.largeTitle)
                        .foregroundColor(.orange)
                    Text(error)
                        .foregroundColor(.secondary)
                }
            } else if let data = profData {
                ScrollView {
                    VStack(alignment: .leading, spacing: 16) {

                        // Header card
                        VStack(alignment: .leading, spacing: 8) {
                            HStack {
                                VStack(alignment: .leading) {
                                    Text(data.departments.joined(separator: ", "))
                                        .font(.caption)
                                        .foregroundColor(.secondary)
                                    Text("\(data.overall.total) total students")
                                        .font(.caption)
                                        .foregroundColor(.secondary)
                                }
                                Spacer()
                                GPABadge(gpa: data.overall.avg_gpa)
                            }
                            .padding(.horizontal)

                            GradeBarChart(summary: data.overall)
                                .padding(.horizontal)
                        }

                        // Courses taught
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Courses Taught")
                                .font(.headline)
                                .padding(.horizontal)

                            ForEach(data.by_course) { course in
                                VStack(alignment: .leading, spacing: 6) {
                                    HStack {
                                        Text(course.course)
                                            .font(.subheadline)
                                            .fontWeight(.semibold)
                                        Spacer()
                                        GPABadge(gpa: course.avg_gpa)
                                    }
                                    GradeBarChart(summary: GradeSummary(
                                        A: course.A, B: course.B, C: course.C,
                                        D: course.D, F: course.F, Q: course.Q,
                                        total: course.total, avg_gpa: course.avg_gpa,
                                        pct_A: course.pct_A, pct_B: course.pct_B,
                                        pct_C: course.pct_C, pct_D: course.pct_D,
                                        pct_F: course.pct_F
                                    ), compact: true)
                                    Text("\(course.sections) section\(course.sections == 1 ? "" : "s") · \(course.total) students")
                                        .font(.caption2)
                                        .foregroundColor(.secondary)
                                }
                                .padding(.horizontal)

                                Divider().padding(.horizontal)
                            }
                        }
                    }
                    .padding(.vertical)
                }
            }
        }
        .navigationTitle(professorName)
        .navigationBarTitleDisplayMode(.large)
        .task { await loadData() }
    }

    func loadData() async {
        isLoading = true
        errorMessage = nil
        do {
            let data = try await APIClient.shared.getProfessor(
                name: professorName,
                department: department
            )
            await MainActor.run {
                profData = data
                isLoading = false
            }
        } catch {
            await MainActor.run {
                errorMessage = "Couldn't load data for \(professorName)."
                isLoading = false
            }
        }
    }
}
