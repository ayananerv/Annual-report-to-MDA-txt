# --- UTILS ---
# START_PATTERNS = cg.PATTERNS["start_patterns"]
# END_PATTERNS = cg.PATTERNS["ending_patterns"]
# START_PAGE_RANGE = cg.IO["search_page_range"]

CN_NUM = {
    "零": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def chinese_to_number(s):
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    if s == "十":
        return 10
    if len(s) == 2 and s.startswith("十"):
        return 10 + CN_NUM.get(s[1], 0)
    if len(s) == 2 and s.endswith("十"):
        return CN_NUM.get(s[0], 1) * 10
    if len(s) == 3 and s[1] == "十":
        return CN_NUM.get(s[0], 0) * 10 + CN_NUM.get(s[2], 0)
    return CN_NUM.get(s, 0)


def number_to_chinese(n):
    if n <= 10:
        return list(CN_NUM.keys())[list(CN_NUM.values()).index(n)]
    if n < 20:
        return "十" + list(CN_NUM.keys())[list(CN_NUM.values()).index(n - 10)]
    if n < 100:
        tens = n // 10
        units = n % 10
        res = list(CN_NUM.keys())[list(CN_NUM.values()).index(tens)] + "十"
        if units > 0:
            res += list(CN_NUM.keys())[list(CN_NUM.values()).index(units)]
        return res
    return str(n)
