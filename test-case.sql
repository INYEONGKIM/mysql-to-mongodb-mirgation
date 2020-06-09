-- Query 1
select name from instructor where dept_name='Comp. Sci.' and salary > 80000;

-- Query 2
select name, course_id from instructor, teaches where instructor.ID = teaches.ID;

-- Query 3
select name, title from instructor natural join teaches natural join course;

-- Query 4-1 (Oracle)
(select course_id from section where semester='Fall' and year=2009) inner join (select course_id from section where semester='Spring' and year=2010);

-- Query 5
select dept_name, avg(salary) from instructor group by dept_name;



-- Query 4-2 (MySQL)
select s1.course_id from section as s1 where s1.semester='Fall' and s1.year=2009
AND s1.course_id IN (select s2.course_id from section as s2 where s2.semester='Spring' and s2.year=2010);


-- Query I join T Full
select i.ID, i.name, i.dept_name, i.salary, t.course_id, t.sec_id, t.semester, t.year from instructor as i, teaches as t where i.ID = t.ID;



-- test
select n.ID, n.course_id, n.sec_id, n.semester, n.year, n.name, n.dept_name, n.salary, n.title, n.credits
from (
	select t.ID, t.course_id, t.sec_id, t.semester, t.year, i.name, i.dept_name, i.salary, c.title, c.credits
	from teaches as t, instructor as i, course as c
	where t.ID = i.ID and t.course_id = c.course_id
) as n


-- New Doc Query 1
select distinct n.name
from (
	select i.ID, i.name, i.dept_name, i.salary, t.course_id, t.sec_id, t.semester, t.year
	from instructor as i left join teaches as t on t.ID=i.ID
) as n
where n.dept_name='Comp. Sci.' and salary > 80000;

-- New Doc Query 2
select n.name, n.course_id
from (
	select i.ID, i.name, i.dept_name, i.salary, t.course_id, t.sec_id, t.semester, t.year
	from instructor as i left join teaches as t on t.ID=i.ID
) as n
where n.course_id IS NOT NULL

-- New Doc Query 5
select sub.dept_name, avg(sub.salary)
from (
    select n.ID, truncate(avg(n.salary), 2) as salary, n.dept_name
    from (
        select i.ID, i.name, i.dept_name, i.salary, t.course_id, t.sec_id, t.semester, t.year
        from instructor as i left join teaches as t on t.ID=i.ID
    ) as n
    group by n.ID
) as sub
group by sub.dept_name


-- query 3
select * from course, (select * from instructor natural join teaches) as n
where (course.course_id = n.course_id) and (course.dept_name = n.dept_name)


-- select * from instructor left join teaches on instructor.ID = teaches.ID where teaches.course_id IS NOT NULL;
-- select * from instructor natural join teaches 두 개 같음 


select i.name, n.name from instructor as i left join (select s.name, a.i_id from student as s left join advisor as a on s.ID=a.s_id) as n on i.ID=n.i_id where i.name='Bertolino';


-- TEST
select inst.name, class.building, class.room_number from 
(select i.ID, i.name, t.course_id, t.sec_id, t.semester, t.year from instructor as i, teaches as t where i.ID = t.ID) as inst,
(select c.building, c.room_number, s.course_id, s.sec_id, s.semester, s.year  from section as s, classroom as c where s.building = c.building and s.room_number = c.room_number) as class
where inst.course_id=class.course_id and inst.sec_id=class.sec_id and inst.semester=class.semester and inst.year=class.year and class.building='Taylor';





