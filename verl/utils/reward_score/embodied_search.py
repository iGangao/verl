import re

''' reference: cogagent
Status: 当前状态
Plan: 计划
Action: 描述action
Operation: NAVIGATE(object="xxxx")
'''
def extract_solution(action_str):
    # this also tests the formatting of the model
    # 匹配出 Operation: 后面的内容
    action = re.search("Operation: (.*?)\n", action_str)
    obj = re.search("object=\"(.*?)\"", action_str)
    if action is None:
        action = None
    else:
        action = action.group(1)  # Changed to group(1) to capture the object
    if obj is None:
        obj = None
    else:
        obj = obj.group(1)    
    return (action, obj)


def compute_score(action_str, ground_truth, format_score=0.1, score=1.):
    """The scoring function for embodied_search.
    Args:
        action_str: the action text
        ground_truth: the ground truth
        format_score: the score for the format
        score: the score for the correct action
    """
    action, obj = extract_solution(action_str=action_str)
    if action is None and obj is None:
        return 0
    else:
        if action == ground_truth and obj == ground_truth:
            return score
        else:
            return format_score * int(action is not None) + format_score * int(obj is not None)