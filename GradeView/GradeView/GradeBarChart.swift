import SwiftUI

/// A horizontal bar chart showing A/B/C/D/F grade distribution.
struct GradeBarChart: View {
    let summary: GradeSummary
    var compact: Bool = false
    var showCounts: Bool = false

    private var bars: [(letter: String, pct: Double, count: Int, color: Color)] {
        [
            ("A", summary.pct_A, summary.A, Color(hex: "#4CAF50")),
            ("B", summary.pct_B, summary.B, Color(hex: "#8BC34A")),
            ("C", summary.pct_C, summary.C, Color(hex: "#FFC107")),
            ("D", summary.pct_D, summary.D, Color(hex: "#FF7043")),
            ("F", summary.pct_F, summary.F, Color(hex: "#F44336")),
        ]
    }

    var body: some View {
        VStack(alignment: .leading, spacing: compact ? 4 : 8) {
            ForEach(bars, id: \.letter) { bar in
                HStack(spacing: 8) {
                    Text(bar.letter)
                        .font(compact ? .caption : .subheadline)
                        .fontWeight(.bold)
                        .foregroundColor(.secondary)
                        .frame(width: 16)

                    GeometryReader { geo in
                        ZStack(alignment: .leading) {
                            RoundedRectangle(cornerRadius: 4)
                                .fill(Color(.systemGray5))
                                .frame(height: compact ? 12 : 18)

                            RoundedRectangle(cornerRadius: 4)
                                .fill(bar.color)
                                .frame(
                                    width: max(geo.size.width * bar.pct / 100, bar.pct > 0 ? 4 : 0),
                                    height: compact ? 12 : 18
                                )
                                .animation(.spring(response: 0.5, dampingFraction: 0.8), value: bar.pct)
                        }
                    }
                    .frame(height: compact ? 12 : 18)

                    Group {
                        if showCounts {
                            Text("\(bar.count)")
                        } else {
                            Text(String(format: "%.1f%%", bar.pct))
                        }
                    }
                    .font(compact ? .caption2 : .caption)
                    .foregroundColor(.secondary)
                    .frame(width: 44, alignment: .trailing)
                    .animation(.easeInOut, value: showCounts)
                }
            }

            if !compact {
                HStack {
                    Label(String(format: "%.3f GPA", summary.avg_gpa), systemImage: "chart.line.uptrend.xyaxis")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    Spacer()
                    Text("\(summary.total) students")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                .padding(.top, 4)
            }
        }
        .padding(compact ? 8 : 12)
        .background(Color(.systemBackground))
        .cornerRadius(10)
    }
}

// MARK: - Color hex helper
extension Color {
    init(hex: String) {
        let hex = hex.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
        var int: UInt64 = 0
        Scanner(string: hex).scanHexInt64(&int)
        let r = Double((int >> 16) & 0xFF) / 255
        let g = Double((int >> 8) & 0xFF) / 255
        let b = Double(int & 0xFF) / 255
        self.init(red: r, green: g, blue: b)
    }
}
