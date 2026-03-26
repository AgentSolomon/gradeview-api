import Foundation

// MARK: - School

struct School: Codable, Identifiable, Equatable {
    let id: String
    let name: String
    let short: String
    let color: String
    let state: String?

    var swiftColor: String { color }
}

struct SchoolsResponse: Codable {
    let schools: [School]
}

// MARK: - API Models

struct GradeSummary: Codable {
    let A, B, C, D, F, Q, total: Int
    let avg_gpa: Double
    let pct_A, pct_B, pct_C, pct_D, pct_F: Double
}

struct ProfessorSummary: Codable, Identifiable {
    var id: String { professor }
    let professor: String
    let sections: Int
    let A, B, C, D, F, Q, total: Int
    let avg_gpa: Double
    let pct_A, pct_B, pct_C, pct_D, pct_F: Double
}

struct CourseResponse: Codable {
    let department: String
    let course: String
    let overall: GradeSummary
    let by_professor: [ProfessorSummary]
    let semesters_available: [String]
    let years_available: [Int]
}

struct SearchResult: Codable, Identifiable {
    var id: String { "\(department)-\(course)" }
    let department: String
    let course: String
    let A, B, C, D, F, Q, total: Int
    let avg_gpa: Double
    let pct_A, pct_B, pct_C, pct_D, pct_F: Double
}

struct SearchResponse: Codable {
    let query: String
    let department: String
    let results: [SearchResult]
}

struct CourseSummary: Codable, Identifiable {
    var id: String { course }
    let course: String
    let sections: Int
    let A, B, C, D, F, Q, total: Int
    let avg_gpa: Double
    let pct_A, pct_B, pct_C, pct_D, pct_F: Double
}

struct ProfessorResponse: Codable {
    let professor: String
    let overall: GradeSummary
    let by_course: [CourseSummary]
    let departments: [String]
}

struct DepartmentsResponse: Codable {
    let departments: [String]
}
