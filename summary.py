import re
from collections import Counter
from datetime import datetime, timedelta

from openai import OpenAI

from config import *
from sheets import SheetClient


GP_POSITIVE_SHEET = "구글플레이 긍정"
GP_NEGATIVE_SHEET = "구글플레이 부정"
GP_NEUTRAL_SHEET = "구글플레이 중립"


class SummaryJob:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.sheet = SheetClient()

    def run(self):
        period, start, end, run = self._period()

        if not run:
            print("주말 - 실행 안함")
            return

        print(f"요약 대상: {period}")

        pos = self._load_kakao(POSITIVE_SHEET, start, end)
        neg = self._load_kakao(NEGATIVE_SHEET, start, end)
        sug = self._load_kakao(SUGGESTION_SHEET, start, end)

        gp_pos = self._load_google_review(GP_POSITIVE_SHEET, start, end)
        gp_neg = self._load_google_review(GP_NEGATIVE_SHEET, start, end)
        gp_neu = self._load_google_review(GP_NEUTRAL_SHEET, start, end)

        all_msgs = pos + neg + sug + gp_pos + gp_neg + gp_neu
        keywords = self._keywords(all_msgs)

        summary = self._ai_summary(
            period=period,
            pos=pos,
            neg=neg,
            sug=sug,
            gp_pos=gp_pos,
            gp_neg=gp_neg,
            gp_neu=gp_neu,
            keywords=keywords,
        )

        row = [
            period,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            len(pos) + len(gp_pos),
            len(neg) + len(gp_neg) + len(gp_neu),
            len(sug),
            len(all_msgs),
            ", ".join([f"{k}({v})" for k, v in keywords]),
            summary,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ]

        self.sheet.upsert_summary(SUMMARY_SHEET, period, row)

    def _period(self):
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        wd = today.weekday()

        if wd == 0:
            start = today - timedelta(days=3)
            end = today
            label = f"{start:%Y-%m-%d}~{(end - timedelta(days=1)):%Y-%m-%d}"
            return label, start, end, True

        if wd in (1, 2, 3, 4):
            start = today - timedelta(days=1)
            end = today
            return start.strftime("%Y-%m-%d"), start, end, True

        return None, None, None, False

    def _load_kakao(self, sheet_name, start, end):
        ws = self.sheet.get_sheet(sheet_name)
        data = ws.get_all_values()

        if not data:
            return []

        header = data[0]
        rows = data[1:]

        d_idx = header.index("날짜")
        t_idx = header.index("시간")
        m_idx = header.index("메시지")

        result = []

        for r in rows:
            try:
                dt = datetime.strptime(f"{r[d_idx]} {r[t_idx]}", "%Y-%m-%d %H:%M")
            except Exception:
                continue

            if start <= dt < end:
                msg = r[m_idx].strip()
                if msg:
                    result.append(msg)

        return result

    def _load_google_review(self, sheet_name, start, end):
        try:
            ws = self.sheet.get_sheet(sheet_name)
            data = ws.get_all_values()
        except Exception:
            return []

        if not data:
            return []

        header = data[0]
        rows = data[1:]

        try:
            d_idx = header.index("created_at_utc")
            m_idx = header.index("content")
        except ValueError:
            return []

        result = []

        for r in rows:
            try:
                dt = datetime.strptime(r[d_idx][:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            if start <= dt < end:
                msg = r[m_idx].strip()
                if msg:
                    result.append(msg)

        return result

    def _keywords(self, msgs):
        counter = Counter()
        normalized_stopwords = {s.lower() for s in STOPWORDS}

        for msg in msgs:
            tokens = re.findall(r"[가-힣A-Za-z0-9]+", msg.lower())

            for t in tokens:
                if len(t) <= 1:
                    continue
                if t in normalized_stopwords:
                    continue
                counter[t] += 1

        return counter.most_common(TOP_N)

    def _safe_preview(self, label, items, limit=50):
        if not items:
            return f"{label}은 별도로 확인되지 않음."
        return items[:limit]

    def _ai_summary(self, period, pos, neg, sug, gp_pos, gp_neg, gp_neu, keywords):
        prompt = f"""
기간: {period}

[카카오톡 동향]
긍정: {self._safe_preview("긍정 의견", pos)}
부정: {self._safe_preview("부정 의견", neg)}
건의: {self._safe_preview("건의 사항", sug)}

[구글플레이 리뷰]
긍정 리뷰: {self._safe_preview("긍정 리뷰", gp_pos)}
부정 리뷰: {self._safe_preview("부정 리뷰", gp_neg)}
중립 리뷰: {self._safe_preview("중립 리뷰", gp_neu)}

키워드: {keywords}

아래 양식을 최대한 유지해서 운영 보고용으로 한국어 요약 작성:

기간: {period}

요약:

1. 긍정 의견
- 카카오톡 긍정 동향과 구글플레이 긍정 리뷰를 함께 참고해 정리
- 표시할 동향이 없다면 자연스럽게 "별도로 확인되지 않음" 형태로 작성

2. 부정 의견
- 카카오톡 부정 동향 + 구글플레이 부정/중립 리뷰를 함께 참고해 정리
- 표시할 동향이 없다면 자연스럽게 "별도로 확인되지 않음" 형태로 작성

3. 건의 사항
- 카카오톡 건의 시트 기준으로 정리
- 표시할 내용이 없다면 "건의 사항은 별도로 확인되지 않음."처럼 작성

4. 키워드 분석
- 주요 반복 키워드와 의미를 짧게 정리
- 키워드가 부족하면 자연스럽게 축약

5. 이슈 TOP 5
- 실제 데이터 기준으로 가장 중요하거나 반복적으로 보이는 이슈를 최대 5개까지 정리
- 빈도, 반복성, 영향도를 함께 고려
- 이슈가 부족하면 5개를 억지로 채우지 말 것

6. 추천 대응
- 운영팀이 바로 참고할 수 있는 대응 방향을 2~4개 정도 제안
- 데이터에 근거한 실무형 액션으로 작성
- 과장 없이 작성

마지막에는 종합적으로 1문단 마무리.

조건:
- 실무 보고용 문장
- 과장 금지
- 같은 말 반복 금지
- 실제 데이터에 근거해서만 작성
- 항목이 비어 있으면 억지로 만들지 말 것
"""

        res = self.client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )

        return res.choices[0].message.content.strip()
