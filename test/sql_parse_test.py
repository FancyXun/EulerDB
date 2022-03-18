from sql_parsing import parse_mysql as parse

sql = "select S.studentNo,studentName,score  from Student S join Score on S.studentNo=Score.studentNo  join Course on " \
      "Course.courseNo=Score.courseNo join (select AVG(score) gg ,courseNo  from Score join student H on " \
      "Score.studentNo=H.studentNo    group by courseNo) G on G.courseNo=Course.courseNo where score>gg limit 10"

result = parse(sql)
print(result)
