import os

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

POSITIVE_SHEET = "긍정 동향"
NEGATIVE_SHEET = "부정 동향"
SUGGESTION_SHEET = "건의"
SUMMARY_SHEET = "동향 요약"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

TOP_N = 5

STOPWORDS = [
    "페어리테일", "퀘스트", "운영", "공지", "이벤트", "업데이트",
    "ㅋㅋ", "ㅎㅎ", "진짜", "너무", "이거", "저거"
]
