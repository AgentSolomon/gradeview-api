# GradeView iOS App — Setup

## When Xcode finishes installing:

1. Open Xcode
2. File → New → Project
3. Choose **iOS → App**
4. Settings:
   - Product Name: `GradeView`
   - Team: your Apple Developer account
   - Bundle ID: `com.gradeview.app` (or whatever you like)
   - Interface: **SwiftUI**
   - Language: **Swift**
5. Save it to this folder: `projects/gradeview/GradeView/`
6. **Delete** the default `ContentView.swift` Xcode creates
7. **Drag in** all the `.swift` files from this folder into the Xcode project

## Files to add:
- `GradeViewApp.swift` — app entry point + tab bar
- `Models.swift` — data models
- `APIClient.swift` — talks to the backend
- `GradeBarChart.swift` — the grade bar chart component
- `SearchView.swift` — main search screen
- `CourseDetailView.swift` — course breakdown by professor
- `ProfessorDetailView.swift` — professor profile

## Start the backend first:
```bash
cd ~/projects/gradeview
python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8765
```

## Testing on simulator:
- `localhost:8765` works fine in the iOS Simulator

## Testing on a real iPhone:
- Change `baseURL` in `APIClient.swift` to your Mac's local IP
- e.g. `http://192.168.x.x:8765`
- Find your IP: System Settings → Wi-Fi → Details
