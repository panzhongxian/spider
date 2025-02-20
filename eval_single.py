import os
import json
from evaluation import Evaluator, Schema, get_sql, get_schema, build_foreign_key_map, build_valid_col_units, \
    rebuild_sql_val, rebuild_sql_col, eval_exec_match


def evaluate_single(g_str, p_str, db, db_dir, table_json):
    evaluator = Evaluator()
    levels = ['easy', 'medium', 'hard', 'extra', 'all']
    partial_types = ['select', 'select(no AGG)', 'where', 'where(no OP)', 'group(no Having)',
                     'group', 'order', 'and/or', 'IUEN', 'keywords']
    entries = []
    scores = {}

    for level in levels:
        scores[level] = {'count': 0, 'partial': {}, 'exact': 0.}
        scores[level]['exec'] = 0
        for type_ in partial_types:
            scores[level]['partial'][type_] = {'acc': 0., 'rec': 0., 'f1': 0., 'acc_count': 0, 'rec_count': 0}

    db = os.path.join(db_dir, db, db + ".sqlite")
    schema = Schema(get_schema(db))
    g_sql = get_sql(schema, g_str)
    hardness = evaluator.eval_hardness(g_sql)
    scores[hardness]['count'] += 1
    scores['all']['count'] += 1

    try:
        p_sql = get_sql(schema, p_str)
    except:
        # If p_sql is not valid, then we will use an empty sql to evaluate with the correct sql
        p_sql = {
            "except": None,
            "from": {
                "conds": [],
                "table_units": []
            },
            "groupBy": [],
            "having": [],
            "intersect": None,
            "limit": None,
            "orderBy": [],
            "select": [
                False,
                []
            ],
            "union": None,
            "where": []
        }

    # rebuild sql for value evaluation
    entry = json.JSONDecoder().decode(table_json)
    kmap = build_foreign_key_map(entry)

    g_valid_col_units = build_valid_col_units(g_sql['from']['table_units'], schema)
    g_sql = rebuild_sql_val(g_sql)
    g_sql = rebuild_sql_col(g_valid_col_units, g_sql, kmap)
    p_valid_col_units = build_valid_col_units(p_sql['from']['table_units'], schema)
    p_sql = rebuild_sql_val(p_sql)
    p_sql = rebuild_sql_col(p_valid_col_units, p_sql, kmap)

    # exec 打分
    exec_score = eval_exec_match(db, p_str, g_str, p_sql, g_sql)
    if exec_score:
        scores[hardness]['exec'] += 1.0
        scores['all']['exec'] += 1.0

    # match 打分
    exact_score = evaluator.eval_exact_match(p_sql, g_sql)
    partial_scores = evaluator.partial_scores
    # if exact_score == 0:
    #     print("{} pred: {}".format(hardness, p_str))
    #     print("{} gold: {}".format(hardness, g_str))
    #     print("")
    scores[hardness]['exact'] += exact_score
    scores['all']['exact'] += exact_score
    for type_ in partial_types:
        if partial_scores[type_]['pred_total'] > 0:
            scores[hardness]['partial'][type_]['acc'] += partial_scores[type_]['acc']
            scores[hardness]['partial'][type_]['acc_count'] += 1
        if partial_scores[type_]['label_total'] > 0:
            scores[hardness]['partial'][type_]['rec'] += partial_scores[type_]['rec']
            scores[hardness]['partial'][type_]['rec_count'] += 1
        scores[hardness]['partial'][type_]['f1'] += partial_scores[type_]['f1']
        if partial_scores[type_]['pred_total'] > 0:
            scores['all']['partial'][type_]['acc'] += partial_scores[type_]['acc']
            scores['all']['partial'][type_]['acc_count'] += 1
        if partial_scores[type_]['label_total'] > 0:
            scores['all']['partial'][type_]['rec'] += partial_scores[type_]['rec']
            scores['all']['partial'][type_]['rec_count'] += 1
        scores['all']['partial'][type_]['f1'] += partial_scores[type_]['f1']

    entries.append({
        'predictSQL': p_str,
        'goldSQL': g_str,
        'hardness': hardness,
        'exact': exact_score,
        'partial': partial_scores
    })

    for level in levels:
        if scores[level]['count'] == 0:
            continue
        # exec 分数计算
        scores[level]['exec'] /= scores[level]['count']

        # match 分数计算
        scores[level]['exact'] /= scores[level]['count']
        for type_ in partial_types:
            if scores[level]['partial'][type_]['acc_count'] == 0:
                scores[level]['partial'][type_]['acc'] = 0
            else:
                scores[level]['partial'][type_]['acc'] = scores[level]['partial'][type_]['acc'] / \
                                                         scores[level]['partial'][type_]['acc_count'] * 1.0
            if scores[level]['partial'][type_]['rec_count'] == 0:
                scores[level]['partial'][type_]['rec'] = 0
            else:
                scores[level]['partial'][type_]['rec'] = scores[level]['partial'][type_]['rec'] / \
                                                         scores[level]['partial'][type_]['rec_count'] * 1.0
            if scores[level]['partial'][type_]['acc'] == 0 and scores[level]['partial'][type_]['rec'] == 0:
                scores[level]['partial'][type_]['f1'] = 1
            else:
                scores[level]['partial'][type_]['f1'] = \
                    2.0 * scores[level]['partial'][type_]['acc'] * scores[level]['partial'][type_]['rec'] / (
                            scores[level]['partial'][type_]['rec'] + scores[level]['partial'][type_]['acc'])

    partial_types = ['select', 'select(no AGG)', 'where', 'where(no OP)', 'group(no Having)',
                     'group', 'order', 'and/or', 'IUEN', 'keywords']

    # count = scores['all']['count']
    exec_score = scores['all']['exec']
    match_score = scores['all']['exact']
    acc_score_dict = {}
    for type_ in partial_types:
        acc_score_dict[type_] = scores['all']['partial'][type_]['acc']

    return exec_score, match_score, acc_score_dict


if __name__ == "__main__":
    table_json = """{"column_names": [[-1, "*"], [0, "club id"], [0, "name"], [0, "manager"], [0, "captain"], [0, "manufacturer"], [0, "sponsor"], [1, "player id"], [1, "name"], [1, "country"], [1, "earnings"], [1, "events number"], [1, "wins count"], [1, "club id"]], "column_names_original": [[-1, "*"], [0, "Club_ID"], [0, "Name"], [0, "Manager"], [0, "Captain"], [0, "Manufacturer"], [0, "Sponsor"], [1, "Player_ID"], [1, "Name"], [1, "Country"], [1, "Earnings"], [1, "Events_number"], [1, "Wins_count"], [1, "Club_ID"]], "column_types": ["text", "number", "text", "text", "text", "text", "text", "number", "text", "text", "number", "number", "number", "number"], "db_id": "soccer_3", "foreign_keys": [[13, 1]], "primary_keys": [1, 7], "table_names": ["club", "player"], "table_names_original": ["club", "player"]}"""
    exec_score, match_score, acc_score_dict = evaluate_single("SELECT count(*) FROM club", "SELECT count(*) FROM club",
                                                             "soccer_3",
                                                             db_dir="data/test_database", table_json=table_json)
    print(exec_score, match_score, acc_score_dict)
