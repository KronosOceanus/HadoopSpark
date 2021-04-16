[TOC]
# spark 在线考试分析案例
## 流程
* 模拟学生学习数据发送到 Kafka
* Structured Streaming 实时分析
* 使用 ALS 算法, Spark Streaming 实时推荐
* 分析/推荐结果存放在 mysql/hbase
* 使用 SparkSQL 离线分析 mysql 中的数据
## 目录结构
* /edu
* /edu/analysis：分析/推荐
* /edu/analysis/batch：SparkSQL 离线分析
* /edu/analysis/streaming：在线分析
* /edu/bean：样例类
* /edu/mock：模拟学生学习数据
* /edu/model：ALS 模型
* /edu/utils：工具类
## 数据样例
```json
{
    "student_id":"学生ID_31",
    "textbook_id":"教材ID_1",
    "grade_id":"年级ID_6",
    "subject_id":"科目ID_2_语文",
    "chapter_id":"章节ID_chapter_3",
    "question_id":"题目ID_1003",
    "score":7,
    "answer_time":"2021-01-09 14:53:28",
    "ts":"Jan 9, 2021 2:53:28 PM"
}
```