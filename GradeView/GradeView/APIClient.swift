import Foundation
import Observation

@Observable
class APIClient {
    static let shared = APIClient()

    let baseURL = "https://web-production-73e78.up.railway.app"

    private let decoder: JSONDecoder = {
        let d = JSONDecoder()
        return d
    }()

    func fetch<T: Decodable>(_ path: String) async throws -> T {
        guard let url = URL(string: baseURL + path) else {
            throw URLError(.badURL)
        }
        let (data, response) = try await URLSession.shared.data(from: url)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw URLError(.badServerResponse)
        }
        return try decoder.decode(T.self, from: data)
    }

    func getSchools() async throws -> [School] {
        let response: SchoolsResponse = try await fetch("/schools")
        return response.schools
    }

    func searchCourses(query: String, school: String = "tamu") async throws -> SearchResponse {
        let encoded = query.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? query
        return try await fetch("/search?q=\(encoded)&school=\(school)")
    }

    func getCourse(department: String, course: String, school: String = "tamu", year: Int? = nil, semester: String? = nil) async throws -> CourseResponse {
        var path = "/course?department=\(department)&course=\(course)&school=\(school)"
        if let y = year { path += "&year=\(y)" }
        if let s = semester { path += "&semester=\(s)" }
        return try await fetch(path)
    }

    func getProfessor(name: String, school: String = "tamu", department: String? = nil) async throws -> ProfessorResponse {
        let encoded = name.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? name
        var path = "/professor?name=\(encoded)&school=\(school)"
        if let d = department { path += "&department=\(d)" }
        return try await fetch(path)
    }

    func getDepartments(school: String = "tamu") async throws -> [String] {
        let response: DepartmentsResponse = try await fetch("/departments?school=\(school)")
        return response.departments
    }
}
