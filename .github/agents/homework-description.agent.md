---
description: 'Generate a full, professional homework README for any Data Engineering Zoomcamp module'
---

Given the homework page (raw markdown URL):
<!-- Replace the URL below with the actual homework page URL -->
`<HOMEWORK_RAW_URL>`

And using this existing README as a style reference:
<!-- Replace the path below with the path to any previous README in this workspace -->
`<REFERENCE_README_PATH>`

Generate a complete `README.md` for the homework and save it to the appropriate module folder in this workspace. Follow these rules:

## Content Rules

1. **Fetch the homework page** from the provided URL and extract:
   - Module title and description
   - Dataset properties (base URL, format, page size, pagination strategy, destination)
   - Setup instructions (all steps)
   - Every question with its **verbatim** text and **verbatim** answer options
   - Submission link

2. **Derive every answer from real data** — never guess:
   - For data questions: write and execute the minimal SQL query against the actual pipeline/dataset
   - For code-behavior questions (e.g., dbt lineage, test behavior): reason from source files and official docs
   - Capture the exact query result (numbers, dates, percentages) and use it to select the correct option

3. **For each question**, produce:
   - The question text (verbatim)
   - All answer options as a checkbox list — mark the correct one with `[x]`, others with `[ ]`
   - The bold **Answer:** value
   - A `SQL Query` fenced code block with the query used
   - A result table with real values from execution
   - An **Explanation** of 3–6 sentences covering: what the query does, why the answer is correct, any data quality observations, and real-world relevance
   - A `> **Note:**` blockquote for any data quality issues found (inconsistent casing, nulls, anomalies)

## Structure Rules

Generate the README in this exact section order — do not omit or reorder:

1. **Header** — module title, course link, module name, dataset name, year
2. **Overview** — 2–4 sentence description + bullet list of 5–8 key learning areas + `### Dataset Information` table
3. **Project Setup** — numbered steps with shell commands and explanations
4. **Project Structure** — directory tree + pipeline metadata table (pipeline name, dataset, destination, write disposition)
5. **Homework Solutions** — one subsection per question following the pattern above
6. **Key Concepts** — 2–4 important technical concepts with code examples
7. **Resources** — table with links to docs, workshop notebook, and submission form

## Formatting Rules

- Bold key numbers, answer values, and important terms
- Use backticks for column names, table names, pipeline names, filenames, CLI commands
- Fenced code blocks must have language tags: `sql`, `bash`, `python`, `json`
- Monetary values: `$X,XXX.XX` format
- Percentages: 2 decimal places with `%`
- Dates: `YYYY-MM-DD HH:MM:SS UTC`
- Large numbers: comma-separated (e.g., `7,235`)
- Horizontal rules (`---`) between major sections and between question blocks

## Output Rules

- Save the file as `README.md` in the correct module folder (e.g., `dlt-workshop/README.md`)
- Overwrite any existing `README.md` in that folder entirely
- Do not create any additional summary or changelog files
