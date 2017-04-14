ALTER TABLE active_versions DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE activity DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE activity_log DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE answer DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE answer_log DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE auth_user DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE course_structure DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE cutoff DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE definitions_cutoff DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE definitions_grader DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE grader DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE involvement DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE openassessment DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE openassessment_log DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE problem_groups DROP partition(dt='${hiveconf:PREV_DATE}');
ALTER TABLE student_courseenrollment DROP partition(dt='${hiveconf:PREV_DATE}');

