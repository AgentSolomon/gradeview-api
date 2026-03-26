import SwiftUI

struct CourseDetailView: View {
    let department: String
    let courseNum: String
    let school: School

    @State private var courseData: CourseResponse?
    @State private var isLoading = true
    @State private var errorMessage: String?
    @State private var selectedYear: Int? = nil
    @State private var selectedSemester: String? = nil
    @State private var sortByGPA: Bool = false
    @State private var showCounts: Bool = false

    var title: String { "\(department) \(courseNum)" }

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
            } else if let data = courseData {
                ScrollView {
                    VStack(alignment: .leading, spacing: 16) {

                        // Overall grade chart
                        VStack(alignment: .leading, spacing: 8) {
                            HStack {
                                Text("Overall Distribution")
                                    .font(.headline)
                                Spacer()
                                Picker("Display", selection: $showCounts) {
                                    Text("%").tag(false)
                                    Text("#").tag(true)
                                }
                                .pickerStyle(.segmented)
                                .frame(width: 80)
                            }
                            .padding(.horizontal)
                            GradeBarChart(summary: data.overall, showCounts: showCounts)
                                .padding(.horizontal)
                        }

                        // Filters
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                FilterChip(
                                    title: "All Years",
                                    isSelected: selectedYear == nil,
                                    action: { selectedYear = nil; reload() }
                                )
                                ForEach(data.years_available.sorted(by: >), id: \.self) { year in
                                    FilterChip(
                                        title: "\(year)",
                                        isSelected: selectedYear == year,
                                        action: { selectedYear = year; reload() }
                                    )
                                }
                            }
                            .padding(.horizontal)
                        }

                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                FilterChip(
                                    title: "All Semesters",
                                    isSelected: selectedSemester == nil,
                                    action: { selectedSemester = nil; reload() }
                                )
                                ForEach(["SPRING", "SUMMER", "FALL"], id: \.self) { sem in
                                    if data.semesters_available.contains(sem) {
                                        FilterChip(
                                            title: sem.capitalized,
                                            isSelected: selectedSemester == sem,
                                            action: { selectedSemester = sem; reload() }
                                        )
                                    }
                                }
                            }
                            .padding(.horizontal)
                        }

                        // By professor
                        VStack(alignment: .leading, spacing: 8) {
                            HStack {
                                Text("By Professor")
                                    .font(.headline)
                                Spacer()
                                Picker("Sort", selection: $sortByGPA) {
                                    Label("A–Z", systemImage: "textformat.abc").tag(false)
                                    Label("GPA", systemImage: "chart.bar.fill").tag(true)
                                }
                                .pickerStyle(.segmented)
                                .frame(width: 140)
                            }
                            .padding(.horizontal)

                            ForEach(data.by_professor.sorted {
                                sortByGPA ? $0.avg_gpa > $1.avg_gpa : $0.professor < $1.professor
                            }) { prof in
                                NavigationLink(destination: ProfessorDetailView(
                                    professorName: prof.professor,
                                    department: department
                                )) {
                                    ProfessorRow(prof: prof, showCounts: showCounts)
                                        .padding(.horizontal)
                                }
                                .buttonStyle(.plain)

                                Divider().padding(.horizontal)
                            }
                        }
                    }
                    .padding(.vertical)
                }
            }
        }
        .navigationTitle(title)
        .navigationBarTitleDisplayMode(.large)
        .task { await loadData() }
    }

    func loadData() async {
        isLoading = true
        errorMessage = nil
        do {
            let data = try await APIClient.shared.getCourse(
                department: department,
                course: courseNum,
                school: school.id,
                year: selectedYear,
                semester: selectedSemester
            )
            await MainActor.run {
                courseData = data
                isLoading = false
            }
        } catch {
            await MainActor.run {
                errorMessage = "Couldn't load data for \(title)."
                isLoading = false
            }
        }
    }

    func reload() {
        Task { await loadData() }
    }
}

struct ProfessorRow: View {
    let prof: ProfessorSummary
    var showCounts: Bool = false

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(prof.professor)
                    .font(.subheadline)
                    .fontWeight(.semibold)
                Spacer()
                GPABadge(gpa: prof.avg_gpa)
            }
            GradeBarChart(summary: GradeSummary(
                A: prof.A, B: prof.B, C: prof.C, D: prof.D, F: prof.F, Q: prof.Q,
                total: prof.total, avg_gpa: prof.avg_gpa,
                pct_A: prof.pct_A, pct_B: prof.pct_B, pct_C: prof.pct_C,
                pct_D: prof.pct_D, pct_F: prof.pct_F
            ), compact: true, showCounts: showCounts)

            Text("\(prof.sections) section\(prof.sections == 1 ? "" : "s") · \(prof.total) students")
                .font(.caption2)
                .foregroundColor(.secondary)
        }
        .padding(.vertical, 4)
    }
}

struct FilterChip: View {
    let title: String
    let isSelected: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(title)
                .font(.caption)
                .fontWeight(isSelected ? .semibold : .regular)
                .foregroundColor(isSelected ? .white : .primary)
                .padding(.horizontal, 12)
                .padding(.vertical, 6)
                .background(isSelected ? Color.blue : Color(.systemGray5))
                .cornerRadius(20)
        }
    }
}
