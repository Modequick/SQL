-- table : grade, student
-- Link : https://www.hackerrank.com/challenges/the-report/problem?isFullScreen=true


with base as (select * from students 
LEFT JOIN grades
ON students.marks  between  grades.min_mark and grades.max_mark ) 

, worthy as (select 
case when grade < 8 then NULL else Name end as name, 
grade,
marks,
row_number() over(partition by grade order by marks desc ) as row_number
from base 
where grade >=8 
)

, not_worthy as (select 
case when grade < 8 then NULL else Name end as name, 
grade,
marks,
row_number() over(partition by grade order by marks asc ) as row_number
from base 
where grade <8 
                )

,final as (select * from worthy 
union all 
select * from not_worthy)

select name, grade, marks from final 
order by grade desc, name asc, row_number asc
