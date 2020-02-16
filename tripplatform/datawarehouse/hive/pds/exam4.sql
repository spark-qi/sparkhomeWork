
SELECT  S,Sname,Sage,Ssex from
(SELECT  tb_stu.S,Sname,Sage,Ssex,sum(
case when tb_sc.C='01' 1 else 0
case when tb_sc.C='02' 1 else 0
case when tb_sc.C!='03' 1 else 0 end) flag
from Student tb_stu INNER JOIN SC tb_sc on
tb_stu.S=tb_sc.S group by tb_stu.S  HAVING flag=3) a


SELECT  S,Sname,avg_score,sum_score
(SELECT tb_stu.S,Sname,AVG (Score) avg_score,
SUM (Score) sum_score,SUM(
case when tb_sc.Score>=90 1 else 0 end) flag
from Student tb_stu INNER JOIN SC tb_sc
on tb_stu.S=tb_sc.S GROUP by tb_sc.S HAVING flag>=1) a


SELECT S,Sname,Sage,Ssex,MAX (Score) max_score
from Student tb_stu INNER JOIN SC tb_sc ON
tb_sc.S=tb_stu.S INNER JOIN Course tb_cou ON
tb_sc.C=tb_cou.C GROUP by tb_sc.C HAVING
tb_cou.Cname="语文" and tb_sc.score=max_score