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

        # ✅ 카카오톡
        pos = self._load_kakao(POSITIVE_SHEET, start, end)
        neg = self._load_kakao(NEGATIVE_SHEET, start, end)
        sug = self._load_kakao(SUGGESTION_SHEET, start, end)

        # ✅ 구글 리뷰 기간 분리
        gp_start, gp_end = self._google_review_period()

        if gp_start and gp_end:
            print(f"구글 리뷰 포함: {gp_start:%Y-%m-%d} ~ {gp_end:%Y-%m-%d}")
            gp_pos = self._load_google_review(GP_POSITIVE_SHEET, gp_start, gp_end)
            gp_neg = self._load_google_review(GP_NEGATIVE_SHEET, gp_start, gp_end)
            gp_neu = self._load_google_review(GP_NEUTRAL_SHEET, gp_start, gp_end)
        else:
            print("구글 리뷰 제외")
            gp_pos, gp_neg, gp_neu = [], [], []

        all_msgs = pos + neg + sug + gp_pos + gp_neg + gp_neu
        keywords = self._keywords(all_msgs)

        summary = self._ai_summary(
            period,
            pos,
            neg,
            sug,
            gp_pos,
            gp_neg,
            gp_neu,
            keywords,
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

    # =====================
    # 기간 계산
    # =====================
    def _period(self):
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        wd = today.weekday()

        if wd == 0:  # 월요일
            start = today - timedelta(days=3)
            end = today
            label = f"{start:%Y-%m-%d}~{(end - timedelta(days=1)):%Y-%m-%d}"
            return label, start, end, True

        if wd in (1, 2, 3, 4):  # 화~금
            start = today - timedelta(days=1)
            end = today
            return start.strftime("%Y-%m-%d"), start, end, True

        return None, None, None, False

    # =====================
    # 구글 리뷰 기간 (핵심 로직)
    # =====================
    def _google_review_period(self):
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        wd = today.weekday()

        # ✅ 월요일만 지난주 월~일
        if wd == 0:
            gp_start = today - timedelta(days=7)
            gp_end = today
            return gp_start, gp_end

        return None, None

    # =====================
    # 데이터 로드
    # =====================
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
            except:
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
        except:
            return []

        if not data:
            return []

        header = data[0]
        rows = data[1:]

        try:
            d_idx = header.index("created_at_utc")
            m_idx = header.index("content")
        except:
            return []

        result = []

        for r in rows:
            try:
                dt = datetime.strptime(r[d_idx][:19], "%Y-%m-%d %H:%M:%S")
            except:
                continue

            if start <= dt < end:
                msg = r[m_idx].strip()
                if msg:
                    result.append(msg)

        return result

    # =====================
    # 키워드
    # =====================
    def _keywords(self, msgs):
        counter = Counter()
        stop = {s.lower() for s in STOPWORDS}

        for msg in msgs:
            tokens = re.findall(r"[가-힣A-Za-z0-9]+", msg.lower())
            for t in tokens:
                if len(t) <= 1 or t in stop:
                    continue
                counter[t] += 1

        return counter.most_common(TOP_N)

    def _safe(self, label, data):
        return data[:50] if data else f"{label}은 별도로 확인되지 않음."

    # =====================
    # AI 요약
    # =====================
    def _ai_summary(self, period, pos, neg, sug, gp_pos, gp_neg, gp_neu, keywords):
        prompt = f"""
기간: {period}

[카카오톡]
긍정: {self._safe("긍정 의견", pos)}
부정: {self._safe("부정 의견", neg)}
건의: {self._safe("건의 사항", sug)}

[구글 리뷰]
긍정: {self._safe("긍정 리뷰", gp_pos)}
부정: {self._safe("부정 리뷰", gp_neg)}
중립: {self._safe("중립 리뷰", gp_neu)}

키워드: {keywords}

아래 형식 유지:

1. 긍정 의견
2. 부정 의견
3. 건의 사항
4. 키워드 분석
5. 이슈 TOP 5
6. 추천 대응

조건:
- 데이터 없으면 "별도로 확인되지 않음"
- 실무 보고용
"""

        res = self.client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )

        return res.choices[0].message.content.strip()
