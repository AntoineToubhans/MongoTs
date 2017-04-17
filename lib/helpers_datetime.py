def get_day_count(year, month):
    return {
        1: 31,
        2: 29 if year % 4 == 0 and year % 400 != 0 else 28,
        3: 31,
        4: 30,
        5: 31,
        6: 30,
        7: 31,
        8: 31,
        9: 30,
        10: 31,
        11: 30,
        12: 31,
    }[month]
