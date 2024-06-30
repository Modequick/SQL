-- link : https://www.hackerrank.com/challenges/harry-potter-and-wands/problem?isFullScreen=true

with base as (select 
 wands.id, 
wands.code, 
wands.coins_needed,
wands.power,
wands_property.age,
wands_property.is_evil          
  from wands  
left join 
wands_property on wands.code = wands_property.code
)

, final as (select 
id, 
age,
coins_needed,
power,
row_number() over(partition by power, age order by coins_needed asc ) as row_number
from base 
where is_evil = 0          
)

select 
id, 
age,
coins_needed,
power
from final 
where row_number = 1 
order by power desc, age desc
