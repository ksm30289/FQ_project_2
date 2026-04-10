import re
from collections import Counter
from datetime import datetime, timedelta

from openai import OpenAI

from config import *
from sheets import SheetClient


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

        pos = self._load(POSITIVE_SHEET, start, end)
        neg = self._load(NEGATIVE_SHEET, start, end)
        sug = self._load(SUGGESTION_SHEET, start, end)

        all_msgs = pos + neg + sug

        keywords = self._keywords(all_msgs)

        summary = self._ai_summary(period, pos, neg, sug, keywords)

        row = [
            period,
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            len(pos),
            len(neg),
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

    def _load(self, sheet_name, start, end):
        ws = self.sheet.get_sheet(sheet_name)
        data = ws.get_all_values()

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

    def _keywords(self, msgs):
        counter = Counter()

        for msg in msgs:
            tokens = re.findall(r"[가-힣A-Za-z0-9]+", msg.lower())

            for t in tokens:
                if len(t) <= 1:
                    continue
                if t in STOPWORDS:
                    continue
                counter[t] += 1

        return counter.most_common(TOP_N)

    def _ai_summary(self, period, pos, neg, sug, keywords):
        prompt = f"""
기간: {period}

긍정: {pos[:50]}
부정: {neg[:50]}
건의: {sug[:50]}

키워드: {keywords}

운영 보고용으로 요약해줘.
"""

        res = self.client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )

        return res.choices[0].message.content.strip()
