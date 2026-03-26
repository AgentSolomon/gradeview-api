# GradeView — Product Roadmap

## Vision
The go-to app for understanding how professors grade — and eventually, the AI tutor that helps you ace their class.

GradeView wins because it has **two things nobody else can combine**: professor grade distribution data + AI-powered study tools built around that specific professor's class. That combination is the moat.

---

## Phase 1 — Grade Distributions ✅ (Submitted App Store March 21, 2026)
The core product. Simple, useful, free.

- Browse professors by school, department, course
- View grade distributions with animated bar charts
- Sort by GPA, filter by year/semester
- Toggle between % and raw student counts
- Schools: TAMU (102K records), UT Austin, UW-Madison
- Multi-school architecture ready

**Monetization:** None until 10K downloads. Then affiliate links (Chegg, Course Hero, Quizlet).

---

## Phase 2 — AI Tutor (Target: Fall 2026 semester launch)

### The Big Idea
Each class gets its own AI tutor that learns from everything a student uploads — and gets smarter the more students contribute. The grade distribution data makes it unique: the tutor knows how hard the professor actually grades, so it calibrates its prep accordingly.

### The Class Profile
A persistent, growing knowledge base for a specific professor's class at a specific school:

**Inputs (student uploads):**
- 📹 Lecture videos → auto-transcribed + slides extracted
- 📄 Syllabus → knows the schedule, topics, weights
- 📝 Past quizzes/tests → learns what this professor actually tests
- 📚 Study guides, notes, handouts
- ❓ Practice problems, homework

**What gets built automatically:**
- Full lecture transcripts
- Visual slide library (maps, buildings, diagrams)
- Key terms and definitions extracted from lectures
- Professor's question patterns (from past exams)
- Topic frequency map (what comes up most)

**Output — personalized study materials:**
- Visual study guide PDF (slides + AI descriptions + exam questions)
- Flashcard decks auto-generated from lectures
- Practice exams modeled on the professor's actual style
- Map quiz mode (show a region → identify the building) — critical for architecture/geography/art history
- "What to focus on" briefing before each exam

### The GradeView Flywheel
This is the feature no competitor can copy:

> Dr. Jones gives 12% A's in ARCH 401.

The AI tutor knows this. It goes deeper, demands more precision, generates harder practice questions. Students in easy-grading classes get broader review; tough graders get rigorous drill. The grade distribution data makes the tutor smarter — and that data is GradeView's exclusive asset.

### The Network Effect
- First student uploads Lectures 1-5 → good tutor
- Second student adds Lectures 6-10 + last year's midterm → great tutor
- By 10 students contributing → tutor knows the course cold
- Shared class profile benefits everyone; contributors earn "Contributor" badge

### Exam Format Detection
Different classes test differently. The tutor learns the format:
- **Visual ID** (architecture/art history): map + 3 images → identify correct building/location
- **Multiple choice**: generate MCQ from lecture content
- **Essay**: generate prompts based on professor's past questions
- **Problem sets**: replicate problem style from uploaded homework

### Technical Pipeline (proven as of March 22, 2026)
1. Video upload → moviepy audio extraction
2. Audio → Whisper transcription (local or Groq API for speed)
3. Video → OpenCV scene detection → visual slides only (skip text/title cards)
4. Slides → Grok Vision → descriptions, building IDs, map locations
5. Transcript + slide data → Grok → structured study notes
6. ReportLab → branded PDF study guide
7. All stored in class profile → gets richer with each upload

**Cost per lecture: ~$0.15–0.20** (mostly Grok Vision for slides)
At scale: 1,000 users × 5 lectures/mo ≈ **$150–200/mo total** — trivially cheap.
As AI costs drop (and they will fast), this becomes nearly free.

---

## Phase 3 — Platform & Network (2027+)

- **Web app** alongside iOS (students on laptops)
- **Professor pages** — aggregate rating, grade trends over time, AI tutor availability
- **School leaderboards** — most-studied courses, most-uploaded classes
- **Study groups** — share class profiles with friends
- **API for universities** — sell anonymized trend data back to schools

---

## Monetization Ladder

| Tier | Price | Features |
|------|-------|----------|
| Free | $0 | Grade distributions, professor search, school browsing |
| Student Pro | $9.99/mo | AI Tutor — unlimited uploads, study guides, flashcards, practice exams |
| Study Group | $6.99/mo/person | Shared class profile, collaborative uploads (2-5 students) |
| Affiliate links | Free tier | Chegg, Course Hero, Quizlet referrals embedded in study guides |

**Comparable:** Chegg $15.95/mo, Course Hero $9.95/mo, Quizlet $7.99/mo — none of them have professor-specific grade data or lecture video processing.

---

## TPIA Data Pipeline (ongoing)
Nationwide public records requests to universities for grade distribution data.

- ✅ 73 universities contacted (March 22, 2026)
- 📬 ~320 total targets in pipeline
- Goal: 50+ schools by App Store feature launch
- See: `tpia/tracker.md` and `tpia/university_targets.csv`

---

## Current Status
- ✅ App submitted to App Store (March 21, 2026)
- ✅ Backend: FastAPI on port 8765, 3 schools live
- ✅ Lecture pipeline: fully proven end-to-end (March 22, 2026)
- ✅ gradeview.app live on Cloudflare Pages
- 🔲 App Store approval pending (~March 26-28)
- 🔲 Backend migration: Turso + Railway (this week)
- 🔲 Phase 2 build: Fall 2026 target

---

## Next Milestones
1. App Store approval → announce, film TikToks
2. Backend migration to Turso + Railway
3. Phase 2 spec → UI mockups for AI Tutor
4. Batch 3 TPIA → keep growing school count
5. First 100 downloads → learn what users actually do
